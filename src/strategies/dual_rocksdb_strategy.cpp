#include "dual_rocksdb_strategy.hpp"
#include "../core/types.hpp"
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/statistics.h>
#include <algorithm>

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
    return true;
}

bool DualRocksDBStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    if (!range_index_db_ || !data_storage_db_) {
        log_error("DualRocksDB databases not initialized");
        return false;
    }
    
    // 如果没有启用批量写入，直接写入
    if (!config_.enable_batch_writing) {
        return write_batch_direct(db, records);
    }
    
    // 批量写入模式
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    for (const auto& record : records) {
        add_to_batch(record);
        
        // 更新访问模式（写入也算访问）
        if (cache_manager_) {
            cache_manager_->update_access_pattern(record.addr_slot, true);
        }
    }
    
    // 检查是否需要刷写批次
    if (batch_dirty_) {
        flush_pending_batches();
    }
    
    total_writes_ += records.size();
    
    // 检查内存压力
    check_memory_pressure();
    
    return true;
}

std::optional<Value> DualRocksDBStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    total_reads_++;
    
    // 如果启用了动态缓存优化，尝试使用缓存
    if (cache_manager_) {
        CacheLevel level = cache_manager_->determine_cache_level(addr_slot);
        
        // L1缓存检查：热点数据
        if (level == CacheLevel::HOT) {
            auto cached = cache_manager_->get_hot_data(addr_slot);
            if (cached) {
                cache_hits_++;
                return cached;
            }
        }
        
        // 获取地址的范围列表
        std::vector<uint32_t> ranges;
        if (level >= CacheLevel::MEDIUM) {
            // 从缓存获取范围列表
            auto cached_ranges = cache_manager_->get_range_list(addr_slot);
            if (cached_ranges) {
                ranges = *cached_ranges;
            }
        }
        
        if (ranges.empty()) {
            // 从数据库获取范围列表
            ranges = get_address_ranges(range_index_db_.get(), addr_slot);
            if (level >= CacheLevel::MEDIUM && !ranges.empty()) {
                cache_manager_->cache_range_list(addr_slot, ranges);
            }
        }
        
        if (ranges.empty()) {
            return std::nullopt;
        }
        
        // 找到最新范围并搜索最新块
        uint32_t latest_range = *std::max_element(ranges.begin(), ranges.end());
        auto latest_block = find_latest_block_in_range(data_storage_db_.get(), latest_range, addr_slot);
        
        if (!latest_block) {
            return std::nullopt;
        }
        
        // 获取值
        auto result = get_value_from_data_db(data_storage_db_.get(), latest_range, addr_slot, *latest_block);
        
        if (result) {
            // 根据缓存级别决定是否缓存结果
            if (level == CacheLevel::HOT) {
                cache_manager_->cache_hot_data(addr_slot, *result);
            } else if (level == CacheLevel::PASSIVE) {
                cache_manager_->cache_passive_data(addr_slot, *result);
            }
            
            cache_hits_++;
        }
        
        return result;
    }
    
    // 未启用动态缓存优化的基本实现
    std::vector<uint32_t> ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 找到最新范围并搜索最新块
    uint32_t latest_range = *std::max_element(ranges.begin(), ranges.end());
    auto latest_block = find_latest_block_in_range(data_storage_db_.get(), latest_range, addr_slot);
    
    if (!latest_block) {
        return std::nullopt;
    }
    
    return get_value_from_data_db(data_storage_db_.get(), latest_range, addr_slot, *latest_block);
}

std::optional<Value> DualRocksDBStrategy::query_historical_value(rocksdb::DB* db, 
                                                                   const std::string& addr_slot, 
                                                                   BlockNum target_block) {
    total_reads_++;
    
    // 参考PageIndexStrategy的逻辑：只查询目标范围
    uint32_t target_range = calculate_range(target_block);
    
    // 检查目标范围是否包含该地址的数据
    std::vector<uint32_t> ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 检查目标范围是否在地址的范围内
    if (std::find(ranges.begin(), ranges.end(), target_range) == ranges.end()) {
        // 如果目标范围没有数据，找到小于目标范围的最大范围
        uint32_t max_valid_range = 0;
        bool found_range = false;
        
        for (uint32_t range : ranges) {
            if (range <= target_range && range > max_valid_range) {
                max_valid_range = range;
                found_range = true;
            }
        }
        
        if (!found_range) {
            return std::nullopt;
        }
        
        target_range = max_valid_range;
    }
    
    // 在目标范围内找到 <= target_block 的最大块号
    auto latest_block = find_latest_block_in_range(data_storage_db_.get(), target_range, addr_slot, target_block);
    
    // 如果在目标范围内没找到，尝试在之前的范围中查找
    while (!latest_block && target_range > 0) {
        target_range--;
        if (std::find(ranges.begin(), ranges.end(), target_range) != ranges.end()) {
            latest_block = find_latest_block_in_range(data_storage_db_.get(), target_range, addr_slot, target_block);
        }
    }
    
    if (latest_block) {
        // 找到了符合条件的块，获取其值
        return get_value_from_data_db(data_storage_db_.get(), target_range, addr_slot, *latest_block);
    }
    
    // 没有找到任何符合条件的值
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

uint64_t DualRocksDBStrategy::get_compaction_bytes_written() const {
    // 从data_storage_db_获取统计信息
    if (!data_storage_db_) return 0;
    
    auto options = data_storage_db_->GetOptions();
    auto statistics = options.statistics;
    if (!statistics) return 0;
    
    return statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
}

uint64_t DualRocksDBStrategy::get_compaction_bytes_read() const {
    // 从data_storage_db_获取统计信息
    if (!data_storage_db_) return 0;
    
    auto options = data_storage_db_->GetOptions();
    auto statistics = options.statistics;
    if (!statistics) return 0;
    
    return statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES);
}

uint64_t DualRocksDBStrategy::get_compaction_count() const {
    // 从data_storage_db_获取统计信息
    if (!data_storage_db_) return 0;
    
    auto options = data_storage_db_->GetOptions();
    auto statistics = options.statistics;
    if (!statistics) return 0;
    
    // 使用compaction相关统计的近似值
    return statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES) / (1024 * 1024); // 粗略估算
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
    // 使用固定10位零填充，平衡内存使用和排序需求
    // 覆盖范围：0-9,999,999,999 (100亿块号，足够区块链使用)
    std::string block_str = std::to_string(block_num);
    if (block_str.length() < 10) {
        block_str.insert(0, 10 - block_str.length(), '0');
    }
    
    return "R" + std::to_string(range_num) + "|" + addr_slot + "|" + block_str;
}


std::optional<BlockNum> DualRocksDBStrategy::find_latest_block_in_range(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot, BlockNum max_block) const {
    std::string prefix = "R" + std::to_string(range_num) + "|" + addr_slot + "|";
    
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(options));
    
    // 计算当前范围的最大块号，避免传递的max_block超出当前范围
    BlockNum range_max_block = (range_num + 1) * config_.range_size - 1;
    BlockNum effective_max_block = std::min(max_block, range_max_block);
    
    // 构造target key: R{range_num}|{addr_slot}|{effective_max_block} (使用10位零填充)
    std::string max_block_str = std::to_string(effective_max_block);
    if (max_block_str.length() < 10) {
        max_block_str.insert(0, 10 - max_block_str.length(), '0');
    }
    std::string target_key = prefix + max_block_str;
    
    // 使用SeekForPrev直接定位到<=target_key的最大key
    it->SeekForPrev(rocksdb::Slice(target_key));
    
    BlockNum latest_valid_block = 0;
    bool found = false;
    
    while (it->Valid()) {
        rocksdb::Slice current_key = it->key();
        std::string_view key_view(current_key.data(), current_key.size());
        
        // 检查key是否以正确的prefix开头（使用string_view避免复制）
        if (key_view.substr(0, prefix.length()) == prefix) {
            // 找到了匹配的key，解析block_number
            BlockNum found_block = extract_block_from_key(std::string(key_view));
            if (found_block <= max_block && found_block > latest_valid_block) {
                latest_valid_block = found_block;
                found = true;
                // 因为是从后向前遍历，第一个找到的就是最大的，可以直接返回
                return latest_valid_block;
            }
        }
        
        // 如果key已经小于prefix，说明没有更匹配的key了
        if (key_view < prefix) {
            break;
        }
        
        it->Prev();
    }
    
    return found ? std::optional<BlockNum>(latest_valid_block) : std::nullopt;
}


std::optional<Value> DualRocksDBStrategy::get_value_from_data_db(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const {
    std::string key = build_data_key(range_num, addr_slot, block_num);
    
    std::string value;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
    
    if (status.ok()) {
        return value;
    }
    
    return std::nullopt;
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
    // 仅在启用动态缓存优化时检查内存压力
    if (!cache_manager_ || !config_.enable_dynamic_cache_optimization) {
        return;
    }
    
    static size_t last_check = 0;
    size_t current_usage = cache_manager_->get_memory_usage();
    
    if (current_usage > config_.max_cache_memory * 0.9) {
        // 内存压力超过90%，触发优化
        optimize_cache_usage();
    }
    
    // 定期清理过期缓存
    cache_manager_->clear_expired();
}

void DualRocksDBStrategy::optimize_cache_usage() {
    if (!cache_manager_) {
        return;
    }
    
    auto current_usage = cache_manager_->get_memory_usage();
    
    if (current_usage > config_.max_cache_memory) {
        // 内存超限，强制清理
        cache_manager_->evict_least_used();
    }
    
    // 动态缓存优化（可选功能）
    if (config_.enable_dynamic_cache_optimization) {
        dynamic_cache_optimization();
    }
}

void DualRocksDBStrategy::dynamic_cache_optimization() {
    // 基于访问模式重新分级缓存条目
    reclassify_cache_entries();
}

void DualRocksDBStrategy::reclassify_cache_entries() {
    // 根据当前的访问频率重新评估缓存级别
    // 这是一个简化的实现，可以根据需要进一步优化
    
    // 定期清理统计信息中的过时条目通过cache_manager_->clear_expired()处理
    if (cache_manager_) {
        cache_manager_->clear_expired();
    }
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
    
    // 为范围索引数据库优化
    if (is_range_index) {
        options.OptimizeForPointLookup(1024 * 1024 * 1024); // 1GB cache
    } else {
        // 为数据存储数据库优化
        options.OptimizeLevelStyleCompaction();
    }
    
    return options;
}

// ===== 批量写入实现 =====

bool DualRocksDBStrategy::write_batch_direct(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    rocksdb::WriteBatch range_batch;
    rocksdb::WriteBatch data_batch;
    
    for (const auto& record : records) {
        // 计算目标范围
        uint32_t range_num = calculate_range(record.block_num);
        
        // 更新范围索引
        update_range_index(range_index_db_.get(), record.addr_slot, range_num);
        
        // 存储数据（带范围前缀）
        std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
        data_batch.Put(data_key, record.value);
    }
    
    // 写入两个数据库
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    auto range_status = range_index_db_->Write(write_options, &range_batch);
    auto data_status = data_storage_db_->Write(write_options, &data_batch);
    
    if (!range_status.ok() || !data_status.ok()) {
        log_error("Failed to write batch directly to DualRocksDB: range={} data={}", 
                  range_status.ToString(), data_status.ToString());
        return false;
    }
    
    return true;
}

void DualRocksDBStrategy::flush_all_batches() {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    if (batch_dirty_) {
        flush_pending_batches();
    }
}

void DualRocksDBStrategy::set_batch_mode(bool enable) {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    
    // 如果从批量模式切换到直接模式，先刷写所有待写入批次
    if (!enable && batch_dirty_) {
        flush_pending_batches();
    }
    
    // 动态修改批量写入配置
    config_.enable_batch_writing = enable;
    
    log_info("Batch mode {}", enable ? "enabled" : "disabled");
}

void DualRocksDBStrategy::add_to_batch(const DataRecord& record) {
    // 计算目标范围
    uint32_t range_num = calculate_range(record.block_num);
    
    // 更新范围索引（直接更新，不缓存范围索引）
    std::vector<uint32_t> current_ranges = get_address_ranges(range_index_db_.get(), record.addr_slot);
    if (std::find(current_ranges.begin(), current_ranges.end(), range_num) == current_ranges.end()) {
        current_ranges.push_back(range_num);
        std::string serialized = serialize_range_list(current_ranges);
        pending_range_batch_.Put(record.addr_slot, serialized);
    }
    
    // 存储数据（带范围前缀）
    std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
    pending_data_batch_.Put(data_key, record.value);
    
    // 更新批次统计
    size_t record_size = record.value.size() + data_key.size() + 100; // 估算大小
    current_batch_size_ += record_size;
    current_batch_blocks_++;
    batch_dirty_ = true;
    
    // 检查是否需要刷写
    if (should_flush_batch(record_size)) {
        flush_pending_batches();
    }
}

bool DualRocksDBStrategy::should_flush_batch(size_t record_size) const {
    // 如果当前批次为空，不需要刷写
    if (current_batch_blocks_ == 0) {
        return false;
    }
    
    // 检查块数限制
    if (current_batch_blocks_ >= config_.batch_size_blocks) {
        return true;
    }
    
    // 检查字节大小限制
    if (current_batch_size_ >= config_.max_batch_size_bytes) {
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
    
    // 保存批次统计信息用于日志
    uint32_t blocks_to_flush = current_batch_blocks_;
    size_t size_to_flush = current_batch_size_;
    
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    // 写入范围索引数据库
    if (pending_range_batch_.Count() > 0) {
        auto range_status = range_index_db_->Write(write_options, &pending_range_batch_);
        if (!range_status.ok()) {
            log_error("Failed to write pending range batch: {}", range_status.ToString());
        }
    }
    
    // 写入数据存储数据库
    if (pending_data_batch_.Count() > 0) {
        auto data_status = data_storage_db_->Write(write_options, &pending_data_batch_);
        if (!data_status.ok()) {
            log_error("Failed to write pending data batch: {}", data_status.ToString());
        }
    }
    
    // 重置批次状态
    pending_range_batch_.Clear();
    pending_data_batch_.Clear();
    current_batch_size_ = 0;
    current_batch_blocks_ = 0;
    batch_dirty_ = false;
    
    log_debug("Flushed batch: {} blocks, {} MB", 
              blocks_to_flush, size_to_flush / (1024 * 1024));
}