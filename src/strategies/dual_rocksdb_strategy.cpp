#include "dual_rocksdb_strategy.hpp"
#include "../core/types.hpp"
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/statistics.h>
#include <algorithm>
#include <chrono>

using namespace utils;

// ===== DualRocksDBStrategy 实现 =====

DualRocksDBStrategy::DualRocksDBStrategy(const Config& config)
    : config_(config) {
    
    // 仅在启用动态缓存优化时初始化缓存管理器
    if (config_.enable_dynamic_cache_optimization) {
        cache_manager_ = std::make_unique<AdaptiveCacheManager>(config.max_cache_memory);
        cache_manager_->set_config(config.hot_cache_ratio, config.medium_cache_ratio);
    }
}

DualRocksDBStrategy::~DualRocksDBStrategy() {
    // 清理资源
    if (range_index_db_) range_index_db_->Close();
    if (data_storage_db_) data_storage_db_->Close();
}

bool DualRocksDBStrategy::initialize(rocksdb::DB* main_db) {
    // 获取主数据库路径 - 从主数据库的GetName()方法获取
    std::string db_path = main_db->GetName();
    if (db_path.empty()) {
        // 如果GetName()返回空，使用默认路径
        main_db->GetEnv()->GetAbsolutePath("./rocksdb_data", &db_path);
    }
    
    // 打开双数据库实例
    rocksdb::Options range_options = get_rocksdb_options(true);
    rocksdb::Options data_options = get_rocksdb_options(false);
    
    std::string range_path = db_path + "_range_index";
    std::string data_path = db_path + "_data_storage";
    
    rocksdb::Status range_status = rocksdb::DB::Open(range_options, range_path, &range_index_db_);
    rocksdb::Status data_status = rocksdb::DB::Open(data_options, data_path, &data_storage_db_);
    
    if (!range_status.ok() || !data_status.ok()) {
        log_error("Failed to open DualRocksDB databases: range={} data={}", range_status.ToString(), data_status.ToString());
        return false;
    }
    
    log_info("DualRocksDBStrategy initialized with range-based partitioning");
    log_info("Using storage strategy: {}", get_strategy_name());
    return true;
}


bool DualRocksDBStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    // Hotspot update模式：每个vector作为1个block，立即写入，不积累
    utils::log_debug("write_batch: Processing {} records as 1 block", records.size());
    
    // 准备WriteBatch
    rocksdb::WriteBatch range_batch;
    rocksdb::WriteBatch data_batch;
    
    // 第一步：处理所有记录，收集range更新和数据
    RangeIndexUpdates range_updates = collect_range_updates_for_hotspot(records);
    add_data_to_batch(records, data_batch);
    
    // 第二步：构建range index更新
    build_range_index_batch(range_updates, range_batch);
    
    // 第三步：立即写入
    bool success = execute_batch_write(range_batch, data_batch, "hotspot_update");
    if (success) {
        total_writes_ += records.size();
        utils::log_debug("write_batch: Successfully wrote {} records", records.size());
    }
    
    return success;
}

bool DualRocksDBStrategy::write_initial_load_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    // Initial load模式：积累多个blocks，达到batch限制后统一写入
    utils::log_debug("write_initial_load_batch: Processing {} records as 1 block", records.size());
    
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    // 计算这个block的大小
    size_t block_size = calculate_block_size(records);
    
    // 添加到pending batches
    add_block_to_pending_batch(records, block_size);
    
    // 更新统计
    current_batch_blocks_++;
    total_writes_ += records.size();
    
    utils::log_debug("write_initial_load_batch: Added block, batch now has {} blocks, {} bytes", 
                     current_batch_blocks_, current_batch_size_);
    
    // 检查是否需要刷写
    if (should_flush_batch(0)) {
        utils::log_info("Flushing batch: {} blocks, {} bytes", current_batch_blocks_, current_batch_size_);
        flush_pending_batches();
    }
    
    return true;
}

std::optional<Value> DualRocksDBStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    total_reads_++;
    
    // 简化实现：直接查询最新值，主要用于接口兼容
    std::vector<uint32_t> ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 找到最新范围并搜索最新块
    uint32_t latest_range = *std::max_element(ranges.begin(), ranges.end());
    return find_latest_block_in_range(data_storage_db_.get(), latest_range, addr_slot);
}

std::optional<Value> DualRocksDBStrategy::query_historical_version(rocksdb::DB* db, 
                                                                    const std::string& addr_slot, 
                                                                    BlockNum target_version) {
    // 实现复杂语义：≤target_version找最新，找不到则找≥的最小值
    total_reads_++;
    
    std::vector<uint32_t> ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 计算target_version所在的range
    uint32_t target_range = calculate_range(target_version);
    
    // 第一步：尝试在≤target_version的ranges中查找最新版本
    std::optional<std::pair<BlockNum, Value>> best_result;
    
    for (uint32_t range_num : ranges) {
        if (range_num > target_range) continue; // 跳过>target_range的范围
        
        auto result = find_latest_block_in_range_with_block(data_storage_db_.get(), range_num, addr_slot, target_version);
        if (result.has_value()) {
            if (!best_result.has_value() || result->first > best_result->first) {
                best_result = result.value();
            }
        }
    }
    
    if (best_result.has_value()) {
        // 找到了≤target_version的最新版本
        return std::to_string(best_result->first) + ":" + best_result->second;
    }
    
    // 第二步：没找到≤target_version的版本，查找≥target_version的最小值
    for (uint32_t range_num : ranges) {
        if (range_num < target_range) continue; // 跳过<target_range的范围
        
        auto result = find_minimum_block_in_range(data_storage_db_.get(), range_num, addr_slot, target_version);
        if (result.has_value()) {
            return result; // 已经是"block_num:value"格式
        }
    }
    
    utils::log_debug("No version found for key {} at or around target version {}", 
                     addr_slot.substr(0, 8), target_version);
    return std::nullopt;
}


bool DualRocksDBStrategy::cleanup(rocksdb::DB* db) {
    // 刷写所有待写入的批次
    flush_all_batches();
    
    if (cache_manager_) {
        cache_manager_->clear_all();
    }
    
    if (range_index_db_) {
        range_index_db_->Close();
        range_index_db_.reset();
    }
    
    if (data_storage_db_) {
        data_storage_db_->Close();
        data_storage_db_.reset();
    }
    
    utils::log_info("DualRocksDBStrategy cleanup completed");
    return true;
}

void DualRocksDBStrategy::set_config(const Config& config) {
    config_ = config;
    if (cache_manager_) {
        cache_manager_->set_config(config.hot_cache_ratio, config.medium_cache_ratio);
    }
}

double DualRocksDBStrategy::get_cache_hit_rate() const {
    uint64_t total = total_reads_.load();
    if (total == 0) return 0.0;
    return static_cast<double>(cache_hits_.load()) / total;
}

uint64_t DualRocksDBStrategy::get_compaction_statistic(rocksdb::Tickers ticker_type) const {
    // 通用方法：从data_storage_db_获取指定的统计信息
    if (!data_storage_db_) return 0;
    
    auto options = data_storage_db_->GetOptions();
    auto statistics = options.statistics;
    if (!statistics) return 0;
    
    return statistics->getTickerCount(ticker_type);
}

uint64_t DualRocksDBStrategy::get_compaction_bytes_written() const {
    return get_compaction_statistic(rocksdb::COMPACT_WRITE_BYTES);
}

uint64_t DualRocksDBStrategy::get_compaction_bytes_read() const {
    return get_compaction_statistic(rocksdb::COMPACT_READ_BYTES);
}

uint64_t DualRocksDBStrategy::get_compaction_count() const {
    // 使用compaction相关统计的近似值
    return get_compaction_statistic(rocksdb::COMPACT_READ_BYTES) / (1024 * 1024);
}

double DualRocksDBStrategy::get_compaction_efficiency() const {
    uint64_t bytes_read = get_compaction_bytes_read();
    if (bytes_read == 0) return 0.0;
    return static_cast<double>(get_compaction_bytes_written()) / bytes_read;
}

// ===== 私有方法实现 =====


uint32_t DualRocksDBStrategy::calculate_range(BlockNum block_num) const {
    return block_num / config_.range_size;
}

std::string DualRocksDBStrategy::build_data_key(uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const {
    return "R" + std::to_string(range_num) + "|" + addr_slot + "|" + format_block_number(block_num);
}

std::string DualRocksDBStrategy::format_block_number(BlockNum block_num) const {
    // 使用固定10位零填充，平衡内存使用和排序需求
    // 覆盖范围：0-9,999,999,999 (100亿块号，足够区块链使用)
    std::string block_str = std::to_string(block_num);
    if (block_str.length() < 10) {
        block_str.insert(0, 10 - block_str.length(), '0');
    }
    return block_str;
}

std::optional<std::pair<BlockNum, Value>> DualRocksDBStrategy::seek_iterator_for_prefix(
    rocksdb::Iterator* it, const std::string& prefix, BlockNum target_block, bool seek_forward) const {
    
    while (it->Valid()) {
        rocksdb::Slice current_key = it->key();
        std::string_view key_view(current_key.data(), current_key.size());
        
        // 检查key是否以正确的prefix开头
        if (key_view.substr(0, prefix.length()) == prefix) {
            BlockNum found_block = extract_block_from_key(std::string(key_view));
            
            // 根据搜索方向检查块号条件
            bool block_matches = seek_forward ? (found_block >= target_block) : (found_block <= target_block);
            
            if (block_matches) {
                rocksdb::Slice value_slice = it->value();
                return std::make_pair(found_block, std::string(value_slice.data(), value_slice.size()));
            }
        }
        
        // 如果key已经超出prefix范围，说明没有找到
        if ((seek_forward && key_view > prefix) || (!seek_forward && key_view < prefix)) {
            break;
        }
        
        // 根据方向移动iterator
        if (seek_forward) {
            it->Next();
        } else {
            it->Prev();
        }
    }
    
    return std::nullopt;
}


std::optional<Value> DualRocksDBStrategy::find_latest_block_in_range(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot, BlockNum max_block) const {
    auto result = find_latest_block_in_range_with_block(db, range_num, addr_slot, max_block);
    if (result.has_value()) {
        return result->second;
    }
    return std::nullopt;
}

std::optional<std::pair<BlockNum, Value>> DualRocksDBStrategy::find_latest_block_in_range_with_block(rocksdb::DB* db, 
                                                                                                uint32_t range_num, 
                                                                                                const std::string& addr_slot, 
                                                                                                BlockNum max_block) const {
    std::string prefix = "R" + std::to_string(range_num) + "|" + addr_slot + "|";
    
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(options));
    
    // 计算当前范围的最大块号，避免传递的max_block超出当前范围
    BlockNum range_max_block = (range_num + 1) * config_.range_size - 1;
    BlockNum effective_max_block = std::min(max_block, range_max_block);
    
    // 构造target key: R{range_num}|{addr_slot}|{effective_max_block}
    std::string target_key = prefix + format_block_number(effective_max_block);
    
    // 使用SeekForPrev直接定位到<=target_key的最大key
    it->SeekForPrev(rocksdb::Slice(target_key));
    
    return seek_iterator_for_prefix(it.get(), prefix, max_block, false);
}



BlockNum DualRocksDBStrategy::extract_block_from_key(const std::string& key) const {
    size_t last_sep = key.rfind('|');
    if (last_sep == std::string::npos) return 0;
    
    std::string block_str = key.substr(last_sep + 1);
    if (block_str.empty()) return 0;
    
    return std::stoull(block_str);
}

std::vector<uint32_t> DualRocksDBStrategy::get_address_ranges(rocksdb::DB* db, const std::string& addr_slot) const {
    std::vector<uint32_t> ranges;
    
    std::string value;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), addr_slot, &value);
    
    if (status.ok()) {
        return deserialize_range_list(value);
    }
    
    return ranges;
}

bool DualRocksDBStrategy::update_range_index(rocksdb::DB* db, const std::string& addr_slot, uint32_t range_num) {
    std::vector<uint32_t> current_ranges = get_address_ranges(db, addr_slot);
    
    // 避免重复添加
    if (std::find(current_ranges.begin(), current_ranges.end(), range_num) == current_ranges.end()) {
        current_ranges.push_back(range_num);
        std::string serialized = serialize_range_list(current_ranges);
        
        rocksdb::Status status = db->Put(rocksdb::WriteOptions(), addr_slot, serialized);
        return status.ok();
    }
    
    return true;
}

std::vector<uint32_t> DualRocksDBStrategy::deserialize_range_list(const std::string& data) const {
    if (data.empty()) {
        return {};
    }
    
    std::vector<uint32_t> ranges;
    size_t count = data.size() / sizeof(uint32_t);
    
    for (size_t i = 0; i < count; ++i) {
        uint32_t range = *reinterpret_cast<const uint32_t*>(data.data() + i * sizeof(uint32_t));
        ranges.push_back(range);
    }
    
    return ranges;
}

std::string DualRocksDBStrategy::serialize_range_list(const std::vector<uint32_t>& ranges) const {
    std::string result;
    result.resize(ranges.size() * sizeof(uint32_t));
    
    for (size_t i = 0; i < ranges.size(); ++i) {
        *reinterpret_cast<uint32_t*>(result.data() + i * sizeof(uint32_t)) = ranges[i];
    }
    
    return result;
}

void DualRocksDBStrategy::check_memory_pressure() {
    // 简化的内存压力检查
    if (!cache_manager_) {
        return;
    }
    
    size_t current_usage = cache_manager_->get_memory_usage();
    if (current_usage > config_.max_cache_memory * 0.9) {
        // 内存压力超过90%，清理缓存
        cache_manager_->evict_least_used();
    }
    
    // 定期清理过期缓存
    cache_manager_->clear_expired();
}


rocksdb::Options DualRocksDBStrategy::get_rocksdb_options(bool is_range_index) const {
    rocksdb::Options options;
    options.create_if_missing = true;
    
    if (config_.enable_compression) {
        // 暂时禁用压缩，避免兼容性问题
        // options.compression = rocksdb::kSnappyCompression;
    }
    
    if (config_.enable_bloom_filters) {
        options.memtable_prefix_bloom_size_ratio = 0.1;
    }
    
    // 启用统计信息收集，复用DBManager的实现
    auto statistics = rocksdb::CreateDBStatistics();
    options.statistics = statistics;
    
    // === 内存优化配置（针对400G内存） ===
    // MemTable配置 - 利用大内存
    options.write_buffer_size = 2ULL * 1024 * 1024 * 1024;      // 2GB per memtable
    options.max_write_buffer_number = 12;                        // 12个memtable = 24GB
    options.min_write_buffer_number_to_merge = 4;                // 4个合并后flush
    
    // WAL优化
    options.max_total_wal_size = 8ULL * 1024 * 1024 * 1024;      // 8GB WAL空间
    options.wal_bytes_per_sync = 64 * 1024 * 1024;              // 64MB WAL同步块
    
    // 并行处理最大化
    options.max_background_compactions = 16;                     // 16个compaction线程
    options.max_background_flushes = 8;                          // 8个flush线程
    options.max_subcompactions = 8;                              // 8路并行compaction
    
    // 并行写入优化
    options.allow_concurrent_memtable_write = true;
    options.enable_write_thread_adaptive_yield = true;
    options.write_thread_max_yield_usec = 100;
    
    // 为范围索引数据库优化
    if (is_range_index) {
        options.OptimizeForPointLookup(128 * 1024 * 1024); // 减少到128MB cache，避免OOM
    } else {
        // 为数据存储数据库优化
        options.OptimizeLevelStyleCompaction();
        // 设置更合理的块缓存大小，避免双数据库内存占用过高
        options.row_cache = rocksdb::NewLRUCache(128 * 1024 * 1024); // 128MB
    }
    
    return options;
}

// ===== 批量写入实现 =====

bool DualRocksDBStrategy::execute_batch_write(rocksdb::WriteBatch& range_batch, rocksdb::WriteBatch& data_batch, const char* operation_name) {
    // 写入两个数据库
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    auto range_status = range_index_db_->Write(write_options, &range_batch);
    auto data_status = data_storage_db_->Write(write_options, &data_batch);
    
    if (!range_status.ok() || !data_status.ok()) {
        log_error("Failed to {} to DualRocksDB: range={} data={}", 
                  operation_name, range_status.ToString(), data_status.ToString());
        return false;
    }
    
    return true;
}

void DualRocksDBStrategy::process_record_for_batch(const DataRecord& record, rocksdb::WriteBatch& range_batch, rocksdb::WriteBatch& data_batch, bool is_initial_load) {
    uint32_t range_num = calculate_range(record.block_num);
    
    if (is_initial_load) {
        // Initial Load优化：直接设置range list
        range_batch.Put(record.addr_slot, serialize_range_list({range_num}));
    }
    
    // 存储数据（带范围前缀）
    data_batch.Put(build_data_key(range_num, record.addr_slot, record.block_num), record.value);
}


void DualRocksDBStrategy::flush_all_batches() {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    if (batch_dirty_) {
        flush_pending_batches();
    }
}


bool DualRocksDBStrategy::should_flush_batch(size_t record_size) const {
    // 检查字节大小限制
    if (current_batch_size_ >= config_.max_batch_size_bytes) {
        return true;
    }
    
    // 检查块数限制（只对initial load模式有效）
    if (current_batch_blocks_ >= config_.batch_size_blocks) {
        return true;
    }
    
    // 如果单个记录就超过限制，立即刷写
    if (record_size > config_.max_batch_size_bytes / 2) {
        return true;
    }
    
    return false;
}


void DualRocksDBStrategy::flush_pending_batches() {
    if (!batch_dirty_ || current_batch_blocks_ == 0) {
        return;
    }
    
    utils::log_info("Flushing batch: {} blocks, {} MB", current_batch_blocks_, 
                   current_batch_size_ / (1024 * 1024));
    
    if (!execute_batch_write(pending_range_batch_, pending_data_batch_, "pending_batch")) {
        log_error("Failed to flush pending batches");
    }
    
    // 重置批次状态
    pending_range_batch_.Clear();
    pending_data_batch_.Clear();
    current_batch_size_ = 0;
    current_batch_blocks_ = 0;
    batch_dirty_ = false;
    
    // 清理批次期间的range索引缓存
    batch_range_cache_.clear();
}

// ===== 重构后的写入辅助方法实现 =====

DualRocksDBStrategy::RangeIndexUpdates 
DualRocksDBStrategy::collect_range_updates_for_hotspot(const std::vector<DataRecord>& records) {
    // 收集hotspot更新时需要的range index更新
    RangeIndexUpdates updates;
    
    // 缓存已查询的ranges，避免重复查询
    std::unordered_map<std::string, std::vector<uint32_t>> cached_ranges;
    
    for (const auto& record : records) {
        uint32_t range_num = calculate_range(record.block_num);
        
        // 获取或查询当前的ranges
        auto it = cached_ranges.find(record.addr_slot);
        if (it == cached_ranges.end()) {
            cached_ranges[record.addr_slot] = get_address_ranges(range_index_db_.get(), record.addr_slot);
            it = cached_ranges.find(record.addr_slot);
        }
        
        std::vector<uint32_t>& current_ranges = it->second;
        
        // 检查是否需要添加新的range
        if (std::find(current_ranges.begin(), current_ranges.end(), range_num) == current_ranges.end()) {
            current_ranges.push_back(range_num);
            updates.ranges_to_update[record.addr_slot] = current_ranges;
        }
        
        // 更新访问模式
        if (cache_manager_) {
            cache_manager_->update_access_pattern(record.addr_slot, true);
        }
    }
    
    return updates;
}

void DualRocksDBStrategy::add_data_to_batch(const std::vector<DataRecord>& records, 
                                           rocksdb::WriteBatch& data_batch) {
    // 将所有数据添加到data_batch
    for (const auto& record : records) {
        uint32_t range_num = calculate_range(record.block_num);
        std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
        data_batch.Put(data_key, record.value);
    }
}

void DualRocksDBStrategy::build_range_index_batch(const RangeIndexUpdates& updates, 
                                                 rocksdb::WriteBatch& range_batch) {
    // 构建range index更新batch
    for (const auto& [addr_slot, ranges] : updates.ranges_to_update) {
        std::string serialized = serialize_range_list(ranges);
        range_batch.Put(addr_slot, serialized);
    }
}

size_t DualRocksDBStrategy::calculate_block_size(const std::vector<DataRecord>& records) const {
    // 计算一个block（即一个vector）的估算大小
    size_t total_size = 0;
    for (const auto& record : records) {
        // 每个记录的大小 = value + key + block_num + 额外开销
        total_size += record.value.size() + record.addr_slot.size() + 
                     sizeof(record.block_num) + 100;
    }
    return total_size;
}

void DualRocksDBStrategy::add_block_to_pending_batch(const std::vector<DataRecord>& records, 
                                                    size_t block_size) {
    // 将整个block添加到pending batches
    for (const auto& record : records) {
        process_record_for_batch(record, pending_range_batch_, pending_data_batch_, true);
    }
    
    // 更新批次统计
    current_batch_size_ += block_size;
    batch_dirty_ = true;
}

// ===== 新增的历史版本查询辅助方法 =====

std::optional<Value> DualRocksDBStrategy::find_minimum_block_in_range(rocksdb::DB* db, 
                                                                      uint32_t range_num, 
                                                                      const std::string& addr_slot, 
                                                                      BlockNum min_block) const {
    std::string prefix = "R" + std::to_string(range_num) + "|" + addr_slot + "|";
    
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(options));
    
    // 构造最小可能的key
    std::string target_key = prefix + format_block_number(min_block);
    
    // Seek到target_key，找到>=target_key的第一个key
    it->Seek(target_key);
    
    auto result = seek_iterator_for_prefix(it.get(), prefix, min_block, true);
    if (result.has_value()) {
        return std::to_string(result->first) + ":" + result->second;
    }
    return std::nullopt;
}