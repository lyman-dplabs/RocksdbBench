#include "direct_version_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include <algorithm>
#include <iomanip>
#include <sstream>

DirectVersionStrategy::DirectVersionStrategy() {
    utils::log_info("DirectVersionStrategy created with default batch config: {} blocks, {} bytes max", 
                    config_.batch_size_blocks, config_.max_batch_size_bytes);
}

DirectVersionStrategy::DirectVersionStrategy(const Config& config) : config_(config) {
    utils::log_info("DirectVersionStrategy created with batch config: {} blocks, {} bytes max", 
                    config_.batch_size_blocks, config_.max_batch_size_bytes);
}

bool DirectVersionStrategy::create_column_families(rocksdb::DB* db) {
    // 简化实现：不使用列族，而是使用键前缀来区分不同类型的数据
    utils::log_info("DirectVersionStrategy initialized - using key prefixes instead of column families");
    utils::log_info("Batch configuration: {} blocks per batch, {} bytes max", 
                    config_.batch_size_blocks, config_.max_batch_size_bytes);
    utils::log_info("Using storage strategy: {}", get_strategy_name());
    return true;
}

bool DirectVersionStrategy::initialize(rocksdb::DB* db) {
    return create_column_families(db);
}

bool DirectVersionStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    // Hotspot update模式：每个vector作为1个block，立即写入，不积累
    utils::log_debug("write_batch: Processing {} records as 1 block", records.size());
    
    // 准备WriteBatch
    rocksdb::WriteBatch batch;
    
    // 添加所有记录到batch
    for (const auto& record : records) {
        std::string version_key = build_version_key(record.addr_slot, record.block_num);
        batch.Put(version_key, record.value);
    }
    
    // 立即写入
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    auto status = db->Write(write_options, &batch);
    
    if (!status.ok()) {
        utils::log_error("Failed to write DirectVersion hotspot batch: {}", status.ToString());
        return false;
    }
    
    total_writes_ += records.size();
    utils::log_debug("write_batch: Successfully wrote {} records", records.size());
    
    return true;
}

bool DirectVersionStrategy::write_initial_load_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    // Initial load模式：积累多个blocks，达到batch限制后统一写入
    utils::log_debug("write_initial_load_batch: Processing {} records as 1 block", records.size());
    
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    // 计算这个block的大小
    size_t block_size = calculate_block_size(records);
    
    // 添加到pending batch
    add_block_to_pending_batch(records, block_size);
    
    // 更新统计
    current_batch_blocks_++;
    total_writes_ += records.size();
    
    utils::log_debug("write_initial_load_batch: Added block, batch now has {} blocks, {} bytes", 
                     current_batch_blocks_, current_batch_size_);
    
    // 检查是否需要刷写
    if (should_flush_batch(0)) {
        utils::log_info("Flushing batch: {} blocks, {} bytes", current_batch_blocks_, current_batch_size_);
        flush_pending_batches(db);
    }
    
    return true;
}

bool DirectVersionStrategy::write_batch_with_processor(rocksdb::DB* db, const std::vector<DataRecord>& records, 
                                                       std::function<void(const DataRecord&)> processor) {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    // 处理每个记录
    for (const auto& record : records) {
        processor(record);
    }
    
    // 刷写所有待写入批次到数据库
    if (batch_dirty_ && current_batch_blocks_ > 0) {
        // 保存批次统计信息用于日志
        uint32_t blocks_to_flush = current_batch_blocks_;
        size_t size_to_flush = current_batch_size_;
        
        rocksdb::WriteOptions write_options;
        write_options.sync = false;
        
        // 写入到数据库
        auto status = db->Write(write_options, &pending_batch_);
        if (!status.ok()) {
            utils::log_error("Failed to flush DirectVersion batch: {}", status.ToString());
            return false;
        }
        
        utils::log_debug("Flushed DirectVersion batch: {} blocks, {} bytes", 
                         blocks_to_flush, size_to_flush);
        
        // 重置批次状态
        pending_batch_.Clear();
        current_batch_size_ = 0;
        current_batch_blocks_ = 0;
        batch_dirty_ = false;
    }
    
    total_writes_ += records.size();
    
    return true;
}

std::optional<Value> DirectVersionStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    // 构建最大版本key用于seek
    std::string max_version_key = build_version_key(addr_slot, UINT64_MAX);
    
    // 直接查找<=max_version_key的最大版本，返回value
    return find_value_by_version(db, max_version_key, addr_slot);
}


std::string DirectVersionStrategy::build_version_key(const std::string& addr_slot, BlockNum version) const {
    // 构建版本索引key: VERSION|address_slot:version
    // 使用固定长度格式确保正确的字典序
    std::ostringstream oss;
    oss << "VERSION|" << addr_slot << ":" << std::setw(16) << std::setfill('0') << std::hex << version;
    return oss.str();
}


std::optional<Value> DirectVersionStrategy::find_value_by_version(rocksdb::DB* db, 
                                                                const std::string& version_key,
                                                                const std::string& addr_slot) {
    // 使用RocksDB的Seek功能找到<=version_key的最大版本，直接返回value
    rocksdb::ReadOptions read_options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options));
    
    it->Seek(version_key);
    
    // 如果seek超出了范围，从最后一个开始
    if (!it->Valid()) {
        it->SeekToLast();
    }
    
    while (it->Valid()) {
        rocksdb::Slice current_key = it->key();
        std::string current_key_str = current_key.ToString();
        
        // 检查key是否以VERSION|addr_slot:开头
        std::string expected_prefix = "VERSION|" + addr_slot + ":";
        if (current_key_str.find(expected_prefix) == 0) {
            // 找到了匹配的key，直接返回value
            return it->value().ToString();
        }
        
        // 如果key已经小于VERSION|addr_slot，说明没有找到
        if (current_key_str < expected_prefix) {
            break;
        }
        
        it->Prev();
    }
    
    return std::nullopt;
}

void DirectVersionStrategy::add_to_batch(const DataRecord& record) {
    // 计算记录大小（key + value）
    std::string version_key = build_version_key(record.addr_slot, record.block_num);
    size_t record_size = version_key.size() + record.value.size();
    
    // 添加到批次
    pending_batch_.Put(version_key, record.value);
    current_batch_size_ += record_size;
    current_batch_blocks_++;
    batch_dirty_ = true;
}

bool DirectVersionStrategy::cleanup(rocksdb::DB* db) {
    // 刷写所有待写入的批次
    std::lock_guard<std::mutex> lock(batch_mutex_);
    if (batch_dirty_ && current_batch_blocks_ > 0) {
        flush_pending_batches(db);
    }
    
    utils::log_info("DirectVersionStrategy cleanup completed");
    return true;
}

// ===== 私有方法实现 =====

bool DirectVersionStrategy::should_flush_batch(size_t record_size) const {
    // 检查字节大小限制
    if (current_batch_size_ >= config_.max_batch_size_bytes) {
        return true;
    }
    
    // 检查块数限制
    if (current_batch_blocks_ >= config_.batch_size_blocks) {
        return true;
    }
    
    // 如果单个记录就超过限制，立即刷写
    if (record_size > config_.max_batch_size_bytes / 2) {
        return true;
    }
    
    return false;
}

void DirectVersionStrategy::flush_pending_batches(rocksdb::DB* db) {
    if (!batch_dirty_ || current_batch_blocks_ == 0) {
        return;
    }
    
    // 保存批次统计信息用于日志
    uint32_t blocks_to_flush = current_batch_blocks_;
    size_t size_to_flush = current_batch_size_;
    
    utils::log_info("Flushing DirectVersion batch: {} blocks, {} bytes", blocks_to_flush, size_to_flush);
    
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    // 写入到数据库
    auto status = db->Write(write_options, &pending_batch_initial_);
    if (!status.ok()) {
        utils::log_error("Failed to flush DirectVersion batch: {}", status.ToString());
    }
    
    // 重置批次状态
    pending_batch_initial_.Clear();
    current_batch_size_ = 0;
    current_batch_blocks_ = 0;
    batch_dirty_ = false;
}

size_t DirectVersionStrategy::calculate_block_size(const std::vector<DataRecord>& records) const {
    // 计算一个block（即一个vector）的估算大小
    size_t total_size = 0;
    for (const auto& record : records) {
        // 每个记录的大小 = value + key + block_num + 额外开销
        std::string version_key = build_version_key(record.addr_slot, record.block_num);
        total_size += version_key.size() + record.value.size() + sizeof(record.block_num) + 100;
    }
    return total_size;
}

void DirectVersionStrategy::add_block_to_pending_batch(const std::vector<DataRecord>& records, size_t block_size) {
    // 将整个block添加到pending batch
    for (const auto& record : records) {
        std::string version_key = build_version_key(record.addr_slot, record.block_num);
        pending_batch_initial_.Put(version_key, record.value);
    }
    
    // 更新批次统计
    current_batch_size_ += block_size;
    batch_dirty_ = true;
}

void DirectVersionStrategy::flush_all_batches() {
    // This method is called by StrategyDBManager, but we need db parameter
    // For now, just log that this method was called
    utils::log_info("DirectVersionStrategy::flush_all_batches called - batch will be flushed on next write or cleanup");
}