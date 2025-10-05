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

bool DirectVersionStrategy::initialize(rocksdb::DB* db) {
    // 保存数据库引用用于flush_all_batches
    db_ref_ = db;
    
    utils::log_info("DirectVersionStrategy initialized - using key prefixes instead of column families");
    utils::log_info("Batch configuration: {} blocks per batch, {} MB max", 
                    config_.batch_size_blocks, config_.max_batch_size_bytes / (1024 * 1024));
    utils::log_info("Using storage strategy: {}", get_strategy_name());
    return true;
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
    
    // 打印当前配置信息
    utils::log_info("DirectVersion batch config: batch_size_blocks={}, max_batch_size_bytes={} MB", 
                    config_.batch_size_blocks, config_.max_batch_size_bytes / (1024 * 1024));
    
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

std::optional<Value> DirectVersionStrategy::query_historical_version(rocksdb::DB* db, 
                                                                    const std::string& addr_slot, 
                                                                    BlockNum target_version) {
    // 实现复杂语义：≤target_version找最新，找不到则找≥的最小值
    
    // 第一步：尝试查找≤target_version的最新版本，同时获取实际的block_num
    std::string target_key = build_version_key(addr_slot, target_version);
    auto result_with_block = find_value_by_version_with_block(db, target_key, addr_slot);
    
    if (result_with_block.has_value()) {
        // 找到了≤target_version的版本，返回"block_num:value"格式
        return result_with_block;
    }
    
    // 第二步：没找到≤target_version的版本，查找≥target_version的最小值
    utils::log_debug("No version <= {} found for key {}, searching for >= minimum", 
                     target_version, addr_slot.substr(0, 8));
    
    auto next_result = find_minimum_ge_version(db, addr_slot, target_version);
    if (next_result.has_value()) {
        return next_result;
    }
    
    utils::log_debug("No version found for key {} at or around target version {}", 
                     addr_slot.substr(0, 8), target_version);
    return std::nullopt;
}


std::string DirectVersionStrategy::build_version_key(const std::string& addr_slot, BlockNum version) const {
    // 构建版本索引key: VERSION|address_slot:version
    // 使用固定长度格式确保正确的字典序
    std::ostringstream oss;
    oss << "VERSION|" << addr_slot << ":" << std::setw(16) << std::setfill('0') << std::dec << version;
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

std::optional<Value> DirectVersionStrategy::find_value_by_version_with_block(rocksdb::DB* db, 
                                                                            const std::string& version_key,
                                                                            const std::string& addr_slot) {
    // 使用RocksDB的Seek功能找到<=version_key的最大版本，返回"block_num:value"格式
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
            // 找到了匹配的key，提取block_num并检查是否<=target_version
            size_t colon_pos = current_key_str.rfind(':');
            if (colon_pos != std::string::npos) {
                std::string block_str = current_key_str.substr(colon_pos + 1);
                BlockNum block_num = std::stoull(block_str);
                
                // 从version_key中提取target_version进行比较
                size_t target_colon_pos = version_key.rfind(':');
                BlockNum target_version = std::stoull(version_key.substr(target_colon_pos + 1));
                
                if (block_num <= target_version) {
                    // 找到了<=target_version的版本
                    return std::to_string(block_num) + ":" + it->value().ToString();
                } else {
                    // 当前的block_num > target_version，需要继续向前找
                    it->Prev();
                    continue;
                }
            }
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

    utils::log_info("=== DirectVersionStrategy Database Property Statistics ===");

    // 获取compaction统计
    std::string compaction_stats;
    if (db->GetProperty("rocksdb.compaction-stats", &compaction_stats)) {
        utils::log_info("Compaction Stats:\n{}", compaction_stats);
    }

    // 获取column family统计
    std::string cf_stats;
    if (db->GetProperty("rocksdb.cfstats", &cf_stats)) {
        utils::log_info("Column Family Stats:\n{}", cf_stats);
    }

    // 获取数据库统计
    std::string db_stats;
    if (db->GetProperty("rocksdb.stats", &db_stats)) {
        utils::log_info("Database Stats:\n{}", db_stats);
    }

    // 获取SST文件统计
    std::string sstables;
    if (db->GetProperty("rocksdb.sstables", &sstables)) {
        utils::log_info("SSTable Stats:\n{}", sstables);
    }

    // 获取内存表统计
    std::string memtable_stats;
    if (db->GetProperty("rocksdb.cur-size-all-mem-tables", &memtable_stats)) {
        utils::log_info("Current MemTable Size: {}", memtable_stats);
    }

    // 获取level统计
    std::string level_stats;
    if (db->GetProperty("rocksdb.levelstats", &level_stats)) {
        utils::log_info("Level Stats:\n{}", level_stats);
    }

    // 获取数据库大小
    std::string db_size;
    if (db->GetProperty("rocksdb.estimate-num-keys", &db_size)) {
        utils::log_info("Estimated Number of Keys: {}", db_size);
    }

    // 获取实时统计
    std::map<std::string, std::string> stats_map;
    if (db->GetMapProperty(rocksdb::DB::Properties::kDBStats, &stats_map)) {
        utils::log_info("=== Real-time Database Statistics ===");
        for (const auto& [key, value] : stats_map) {
            utils::log_info("{}: {}", key, value);
        }
    }

    // 打印详细的compaction统计
    auto options = db->GetOptions();
    auto statistics = options.statistics.get();
    if (statistics) {
        utils::print_compaction_statistics("DirectVersionStrategy", statistics);
    }

    utils::log_info("================================================");
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
    if (!db_ref_) {
        utils::log_error("DirectVersionStrategy::flush_all_batches called but db_ref_ is null!");
        return;
    }
    
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    if (batch_dirty_ && current_batch_blocks_ > 0) {
        // 保存批次统计信息用于日志
        uint32_t blocks_to_flush = current_batch_blocks_;
        size_t size_to_flush = current_batch_size_;
        
        utils::log_info("Flushing DirectVersion final batch: {} blocks, {} MB", 
                        blocks_to_flush, size_to_flush / (1024 * 1024));
        
        rocksdb::WriteOptions write_options;
        write_options.sync = false;
        
        // 写入初始加载的pending batch
        if (pending_batch_initial_.Count() > 0) {
            utils::log_info("Flushing initial load batch with {} operations", pending_batch_initial_.Count());
            auto status = db_ref_->Write(write_options, &pending_batch_initial_);
            if (!status.ok()) {
                utils::log_error("Failed to flush initial load batch: {}", status.ToString());
            } else {
                utils::log_info("Initial load batch flushed successfully");
            }
        }
        
        // 写入常规pending batch
        if (pending_batch_.Count() > 0) {
            utils::log_info("Flushing regular batch with {} operations", pending_batch_.Count());
            auto status = db_ref_->Write(write_options, &pending_batch_);
            if (!status.ok()) {
                utils::log_error("Failed to flush regular batch: {}", status.ToString());
            } else {
                utils::log_info("Regular batch flushed successfully");
            }
        }
        
        // 重置批次状态
        pending_batch_.Clear();
        pending_batch_initial_.Clear();
        current_batch_size_ = 0;
        current_batch_blocks_ = 0;
        batch_dirty_ = false;
        
        utils::log_info("All DirectVersion batches flushed successfully");
    } else {
        utils::log_info("No pending DirectVersion batches to flush");
    }
}

// ===== 新增的历史版本查询辅助方法 =====


std::optional<Value> DirectVersionStrategy::find_minimum_ge_version(rocksdb::DB* db, 
                                                                   const std::string& addr_slot, 
                                                                   BlockNum target_version) {
    // 查找≥target_version的最小版本
    
    std::string prefix = "VERSION|" + addr_slot + ":";
    std::string target_key = build_version_key(addr_slot, target_version);
    
    rocksdb::ReadOptions read_options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options));
    
    it->Seek(target_key);
    
    // SeekForPrev会找到<=target_key的，我们需要>=的，所以用Seek
    if (!it->Valid()) {
        // 如果seek到了最后，说明没有≥target_version的版本
        return std::nullopt;
    }
    
    // 检查第一个有效的key是否≥target_version
    rocksdb::Slice current_key = it->key();
    std::string current_key_str = current_key.ToString();
    
    if (current_key_str.find(prefix) == 0) {
        // 这个key就是≥target_version的最小版本
        rocksdb::Slice value_slice = it->value();
        auto value = value_slice.ToString();
        
        // 提取block_num
        size_t colon_pos = current_key_str.rfind(':');
        if (colon_pos != std::string::npos) {
            std::string block_str = current_key_str.substr(colon_pos + 1);
            auto block_num = std::stoull(block_str);
            return std::to_string(block_num) + ":" + value;
        }
        
        return value;
    }
    
    // 如果第一个key不匹配prefix，可能需要向前或向后查找
    utils::log_debug("First key after seek doesn't match prefix: {}", current_key_str);
    return std::nullopt;
}