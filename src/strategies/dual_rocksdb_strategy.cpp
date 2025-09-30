#include "dual_rocksdb_strategy.hpp"
#include "../core/types.hpp"
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
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
    // 获取主数据库路径
    std::string db_path;
    main_db->GetEnv()->GetAbsolutePath("./rocksdb_data", &db_path);
    
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
    
    rocksdb::WriteBatch range_batch;
    rocksdb::WriteBatch data_batch;
    
    for (const auto& record : records) {
        // 计算目标范围
        uint32_t range_num = calculate_range(record.block_num);
        
        // 更新范围索引
        update_range_index(db, record.addr_slot, range_num);
        
        // 存储数据（带范围前缀）
        std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
        data_batch.Put(data_key, record.value);
        
        // 更新访问模式（写入也算访问）
        if (cache_manager_) {
            cache_manager_->update_access_pattern(record.addr_slot, true);
        }
    }
    
    // 写入两个数据库
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    auto range_status = range_index_db_->Write(write_options, &range_batch);
    auto data_status = data_storage_db_->Write(write_options, &data_batch);
    
    if (!range_status.ok() || !data_status.ok()) {
        log_error("Failed to write batch to DualRocksDB: range={} data={}", range_status.ToString(), data_status.ToString());
        return false;
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
    
    // 获取地址的所有范围
    std::vector<uint32_t> ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    if (ranges.empty()) {
        return std::nullopt;
    }
    
    // 计算目标范围
    uint32_t target_range = calculate_range(target_block);
    
    // 检查目标范围是否在地址的修改范围内
    if (std::find(ranges.begin(), ranges.end(), target_range) == ranges.end()) {
        // 地址在目标范围内没有修改，返回前一个修改范围的最新值
        return std::nullopt;
    }
    
    // 在目标范围内找到 <= target_block 的最大块号
    auto latest_block = find_latest_block_in_range(data_storage_db_.get(), target_range, addr_slot, target_block);
    if (!latest_block) {
        return std::nullopt;
    }
    
    // 获取对应块的值
    return get_value_from_data_db(data_storage_db_.get(), target_range, addr_slot, *latest_block);
}

bool DualRocksDBStrategy::cleanup(rocksdb::DB* db) {
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

// ===== 私有方法实现 =====

uint32_t DualRocksDBStrategy::calculate_range(BlockNum block_num) const {
    return block_num / config_.range_size;
}

std::string DualRocksDBStrategy::build_data_key(uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const {
    return "R" + std::to_string(range_num) + "|" + addr_slot + "|B" + std::to_string(block_num);
}

std::optional<BlockNum> DualRocksDBStrategy::find_latest_block_in_range(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot) const {
    std::string prefix = "R" + std::to_string(range_num) + "|" + addr_slot + "|B";
    
    // 使用seek_last找到最新块
    rocksdb::ReadOptions options;
    rocksdb::Iterator* it = db->NewIterator(options);
    
    std::string end_key = prefix + "FFFFFFFF";
    it->SeekForPrev(end_key);
    
    std::optional<BlockNum> result;
    if (it->Valid() && it->key().starts_with(prefix)) {
        result = extract_block_from_key(it->key().ToString());
    }
    
    delete it;
    return result;
}

std::optional<BlockNum> DualRocksDBStrategy::find_latest_block_in_range(rocksdb::DB* db, uint32_t range_num, const std::string& addr_slot, BlockNum max_block) const {
    std::string prefix = "R" + std::to_string(range_num) + "|" + addr_slot + "|B";
    
    rocksdb::ReadOptions options;
    rocksdb::Iterator* it = db->NewIterator(options);
    
    // 构造最大可能的key
    std::string max_key = prefix + std::to_string(max_block);
    it->SeekForPrev(max_key);
    
    std::optional<BlockNum> result;
    if (it->Valid() && it->key().starts_with(prefix)) {
        BlockNum found_block = extract_block_from_key(it->key().ToString());
        if (found_block <= max_block) {
            result = found_block;
        }
    }
    
    delete it;
    return result;
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
    
    // 为范围索引数据库优化
    if (is_range_index) {
        options.OptimizeForPointLookup(1024 * 1024 * 1024); // 1GB cache
    } else {
        // 为数据存储数据库优化
        options.OptimizeLevelStyleCompaction();
    }
    
    return options;
}