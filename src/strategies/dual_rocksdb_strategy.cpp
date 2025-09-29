#include "dual_rocksdb_strategy.hpp"
#include <rocksdb/options.h>
#include <rocksdb/filter_policy.h>
#include <algorithm>
#include <chrono>
#include <thread>

// ===== AdaptiveCacheManager 实现 =====

AdaptiveCacheManager::AdaptiveCacheManager(size_t max_memory_bytes)
    : max_memory_limit_(max_memory_bytes) {
}

void AdaptiveCacheManager::cache_hot_data(const std::string& key, const Value& value) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = estimate_entry_size(value);
    update_memory_usage(entry_size);
    
    hot_cache_[key] = {value, std::chrono::system_clock::now(), std::chrono::system_clock::now()};
    update_access_pattern(key, true);
}

void AdaptiveCacheManager::cache_range_list(const std::string& key, const std::vector<uint32_t>& ranges) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = ranges.size() * sizeof(uint32_t) + key.size();
    update_memory_usage(entry_size);
    
    range_cache_[key] = ranges;
    update_access_pattern(key, true);
}

void AdaptiveCacheManager::cache_passive_data(const std::string& key, const Value& value) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    size_t entry_size = estimate_entry_size(value);
    update_memory_usage(entry_size);
    
    passive_cache_[key] = value;
    update_access_pattern(key, true);
}

std::optional<Value> AdaptiveCacheManager::get_hot_data(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = hot_cache_.find(key);
    if (it != hot_cache_.end()) {
        it->second.last_access = std::chrono::system_clock::now();
        update_access_pattern(key, false);
        return it->second.value;
    }
    return std::nullopt;
}

std::optional<std::vector<uint32_t>> AdaptiveCacheManager::get_range_list(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = range_cache_.find(key);
    if (it != range_cache_.end()) {
        update_access_pattern(key, false);
        return it->second;
    }
    return std::nullopt;
}

std::optional<Value> AdaptiveCacheManager::get_passive_data(const std::string& key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = passive_cache_.find(key);
    if (it != passive_cache_.end()) {
        update_access_pattern(key, false);
        return it->second;
    }
    return std::nullopt;
}

void AdaptiveCacheManager::update_access_pattern(const std::string& key, bool is_write) {
    auto now = std::chrono::system_clock::now();
    auto& stats = access_stats_[key];
    
    stats.access_count++;
    stats.last_access = now;
    if (stats.first_access == std::chrono::system_clock::time_point{}) {
        stats.first_access = now;
    }
}

CacheLevel AdaptiveCacheManager::determine_cache_level(const std::string& key) const {
    auto stats_it = access_stats_.find(key);
    if (stats_it == access_stats_.end()) {
        return CacheLevel::PASSIVE;
    }
    
    const auto& stats = stats_it->second;
    auto now = std::chrono::system_clock::now();
    auto age = std::chrono::duration_cast<std::chrono::seconds>(now - stats.last_access);
    
    // 基于访问频率和内存压力动态判断
    double access_frequency = stats.access_count / std::max(1.0, static_cast<double>(age.count()));
    
    if (access_frequency > 100.0 && has_memory_for_hot_data()) {
        return CacheLevel::HOT;
    } else if (access_frequency > 10.0 && has_memory_for_medium_data()) {
        return CacheLevel::MEDIUM;
    } else {
        return CacheLevel::PASSIVE;
    }
}

size_t AdaptiveCacheManager::get_memory_usage() const {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    return current_memory_usage_;
}

void AdaptiveCacheManager::manage_memory_pressure() {
    if (!enable_memory_monitor_) return;
    
    if (current_memory_usage_ > max_memory_limit_ * 0.9) {
        evict_least_used();
    }
}

void AdaptiveCacheManager::evict_least_used() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    // 清理被动缓存
    if (current_memory_usage_ > max_memory_limit_ * 0.8) {
        // 简化清理逻辑，直接清理50%的缓存
        size_t to_remove = passive_cache_.size() / 2;
        auto it = passive_cache_.begin();
        for (size_t i = 0; i < to_remove && it != passive_cache_.end(); ++i) {
            it = passive_cache_.erase(it);
        }
    }
    
    // 清理中等缓存
    if (current_memory_usage_ > max_memory_limit_ * 0.6) {
        // 简化清理逻辑，直接清理30%的缓存
        size_t to_remove = range_cache_.size() * 0.3;
        auto it = range_cache_.begin();
        for (size_t i = 0; i < to_remove && it != range_cache_.end(); ++i) {
            it = range_cache_.erase(it);
        }
    }
    
    // 重新计算内存使用
    current_memory_usage_ = 0;
    for (const auto& [key, entry] : hot_cache_) {
        current_memory_usage_ += estimate_entry_size(entry.value);
    }
    for (const auto& [key, ranges] : range_cache_) {
        current_memory_usage_ += ranges.size() * sizeof(uint32_t) + key.size();
    }
    for (const auto& [key, value] : passive_cache_) {
        current_memory_usage_ += estimate_entry_size(value);
    }
}

void AdaptiveCacheManager::set_config(double hot_ratio, double medium_ratio) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    hot_cache_ratio_ = hot_ratio;
    medium_cache_ratio_ = medium_ratio;
}

void AdaptiveCacheManager::clear_expired() {
    // 实现TTL清理逻辑
    auto now = std::chrono::system_clock::now();
    
    // 清理1小时未访问的热点缓存
    for (auto it = hot_cache_.begin(); it != hot_cache_.end();) {
        auto age = std::chrono::duration_cast<std::chrono::seconds>(now - it->second.last_access);
        if (age.count() > 3600) {
            it = hot_cache_.erase(it);
        } else {
            ++it;
        }
    }
    
    // 清理30分钟未访问的中等缓存
    for (auto it = range_cache_.begin(); it != range_cache_.end();) {
        auto stats_it = access_stats_.find(it->first);
        if (stats_it != access_stats_.end()) {
            auto age = std::chrono::duration_cast<std::chrono::seconds>(now - stats_it->second.last_access);
            if (age.count() > 1800) {
                it = range_cache_.erase(it);
            } else {
                ++it;
            }
        } else {
            ++it;
        }
    }
}

void AdaptiveCacheManager::clear_all() {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    hot_cache_.clear();
    range_cache_.clear();
    passive_cache_.clear();
    access_stats_.clear();
    current_memory_usage_ = 0;
}

bool AdaptiveCacheManager::has_memory_for_hot_data() const {
    return current_memory_usage_ < max_memory_limit_ * hot_cache_ratio_;
}

bool AdaptiveCacheManager::has_memory_for_medium_data() const {
    return current_memory_usage_ < max_memory_limit_ * medium_cache_ratio_;
}

void AdaptiveCacheManager::update_memory_usage(size_t added_bytes) {
    current_memory_usage_ += added_bytes;
    manage_memory_pressure();
}

size_t AdaptiveCacheManager::estimate_entry_size(const Value& value) const {
    return value.size() + sizeof(CacheEntry) + 64; // 额外开销估算
}

// ===== DualRocksDBStrategy 实现 =====

DualRocksDBStrategy::DualRocksDBStrategy(const Config& config)
    : config_(config) {
    
    // 初始化缓存管理器
    cache_manager_ = std::make_unique<AdaptiveCacheManager>(config.max_cache_memory);
    cache_manager_->set_config(config.hot_cache_ratio, config.medium_cache_ratio);
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
    if (!open_databases(db_path)) {
        utils::log_error("Failed to open dual RocksDB instances");
        return false;
    }
    
    utils::log_info("DualRocksDBStrategy initialized successfully");
    utils::log_info("RangeIndexDB: {}", get_range_index_db_path(db_path));
    utils::log_info("DataStorageDB: {}", get_data_storage_db_path(db_path));
    
    return true;
}

bool DualRocksDBStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    rocksdb::WriteBatch range_batch;
    rocksdb::WriteBatch data_batch;
    
    for (const auto& record : records) {
        uint32_t range_num = calculate_range(record.block_num);
        
        // 更新范围索引
        update_range_index(db, record.addr_slot, range_num);
        
        // 存储数据（带范围前缀）
        std::string data_key = build_data_key(range_num, record.addr_slot, record.block_num);
        data_batch.Put(data_key, record.value);
        
        // 更新访问模式（写入也算访问）
        cache_manager_->update_access_pattern(record.addr_slot, true);
    }
    
    // 写入两个数据库
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    auto range_status = range_index_db_->Write(write_options, &range_batch);
    auto data_status = data_storage_db_->Write(write_options, &data_batch);
    
    if (!range_status.ok() || !data_status.ok()) {
        utils::log_error("Write failed - RangeDB: {}, DataDB: {}", 
                        range_status.ToString(), data_status.ToString());
        return false;
    }
    
    total_writes_ += records.size();
    
    // 检查内存压力
    check_memory_pressure();
    
    return true;
}

std::optional<Value> DualRocksDBStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    total_reads_++;
    
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

std::optional<Value> DualRocksDBStrategy::query_historical_value(rocksdb::DB* db, 
                                                                   const std::string& addr_slot, 
                                                                   BlockNum target_block) {
    total_reads_++;
    
    // 获取地址的范围列表
    auto ranges = get_address_ranges(range_index_db_.get(), addr_slot);
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
    std::string prefix = build_data_prefix(target_range, addr_slot);
    std::string seek_key = build_data_key(target_range, addr_slot, target_block);
    
    rocksdb::ReadOptions read_options;
    auto it = std::unique_ptr<rocksdb::Iterator>(data_storage_db_->NewIterator(read_options));
    
    it->SeekForPrev(seek_key);
    
    if (it->Valid() && it->key().starts_with(prefix)) {
        BlockNum found_block = extract_block_from_key(it->key().ToString());
        if (found_block <= target_block) {
            return it->value().ToString();
        }
    }
    
    return std::nullopt;
}

bool DualRocksDBStrategy::cleanup(rocksdb::DB* db) {
    cache_manager_->clear_all();
    
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
    cache_manager_->set_config(config.hot_cache_ratio, config.medium_cache_ratio);
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
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%06d|%s|%012lu", range_num, addr_slot.c_str(), block_num);
    return std::string(buffer);
}

std::string DualRocksDBStrategy::build_data_prefix(uint32_t range_num, const std::string& addr_slot) const {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%06d|%s|", range_num, addr_slot.c_str());
    return std::string(buffer);
}

std::optional<BlockNum> DualRocksDBStrategy::find_latest_block_in_range(rocksdb::DB* db, 
                                                                     uint32_t range_num, 
                                                                     const std::string& addr_slot) const {
    // 双RocksDB策略必须使用Seek-Last优化，这是核心查找机制
    std::string prefix = build_data_prefix(range_num, addr_slot);
    std::string seek_key = prefix + "FFFFFFFFFFFFFFFF"; // 最大可能的block_num
    
    rocksdb::ReadOptions options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(options));
    
    // 直接定位到可能的最大键位置
    it->SeekForPrev(seek_key);
    
    if (it->Valid() && it->key().starts_with(prefix)) {
        return extract_block_from_key(it->key().ToString());
    }
    
    return std::nullopt;
}

bool DualRocksDBStrategy::update_range_index(rocksdb::DB* db, const std::string& addr_slot, uint32_t range_num) {
    // 获取现有的范围列表
    auto existing_ranges = get_address_ranges(range_index_db_.get(), addr_slot);
    
    // 检查是否已存在
    if (std::find(existing_ranges.begin(), existing_ranges.end(), range_num) != existing_ranges.end()) {
        return true; // 已存在，无需更新
    }
    
    // 添加新范围
    existing_ranges.push_back(range_num);
    std::sort(existing_ranges.begin(), existing_ranges.end());
    
    // 序列化并存储
    std::string serialized = serialize_range_list(existing_ranges);
    rocksdb::Status status = range_index_db_->Put(rocksdb::WriteOptions(), addr_slot, serialized);
    
    return status.ok();
}

std::vector<uint32_t> DualRocksDBStrategy::get_address_ranges(rocksdb::DB* db, const std::string& addr_slot) const {
    std::string serialized;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), addr_slot, &serialized);
    
    if (!status.ok()) {
        return {};
    }
    
    return deserialize_range_list(serialized);
}

bool DualRocksDBStrategy::open_databases(const std::string& base_path) {
    std::string range_path = get_range_index_db_path(base_path);
    std::string data_path = get_data_storage_db_path(base_path);
    
    // 创建范围索引数据库
    if (!create_range_index_db(range_path)) {
        return false;
    }
    
    // 创建数据存储数据库
    if (!create_data_storage_db(data_path)) {
        return false;
    }
    
    return true;
}

bool DualRocksDBStrategy::create_range_index_db(const std::string& path) {
    rocksdb::Options options = get_rocksdb_options(true); // 范围索引专用配置
    
    rocksdb::Status status = rocksdb::DB::Open(options, path, &range_index_db_);
    if (!status.ok()) {
        utils::log_error("Failed to open RangeIndexDB at {}: {}", path, status.ToString());
        return false;
    }
    
    return true;
}

bool DualRocksDBStrategy::create_data_storage_db(const std::string& path) {
    rocksdb::Options options = get_rocksdb_options(false); // 数据存储专用配置
    
    rocksdb::Status status = rocksdb::DB::Open(options, path, &data_storage_db_);
    if (!status.ok()) {
        utils::log_error("Failed to open DataStorageDB at {}: {}", path, status.ToString());
        return false;
    }
    
    return true;
}

std::optional<Value> DualRocksDBStrategy::get_value_from_data_db(rocksdb::DB* db, 
                                                                 uint32_t range_num, 
                                                                 const std::string& addr_slot, 
                                                                 BlockNum block_num) {
    std::string key = build_data_key(range_num, addr_slot, block_num);
    std::string value;
    
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), key, &value);
    
    if (status.ok()) {
        return value;
    }
    
    return std::nullopt;
}

BlockNum DualRocksDBStrategy::extract_block_from_key(const std::string& key) const {
    // 键格式: {range:06d}|{addr_slot}|{block_num:012lu}
    size_t last_sep = key.rfind('|');
    if (last_sep == std::string::npos) {
        return 0;
    }
    
    return std::stoull(key.substr(last_sep + 1));
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
    cache_manager_->clear_expired();
}

rocksdb::Options DualRocksDBStrategy::get_rocksdb_options(bool is_range_index) const {
    rocksdb::Options options;
    options.create_if_missing = true;
    
    // 基础配置
    options.max_open_files = -1;
    options.use_fsync = false;
    options.stats_dump_period_sec = 60;
    
    // 压缩配置 - 暂时禁用压缩以避免兼容性问题
    if (config_.enable_compression) {
        options.compression = rocksdb::kNoCompression; // 暂时禁用压缩
    } else {
        options.compression = rocksdb::kNoCompression;
    }
    
    // 布隆过滤器配置
    if (config_.enable_bloom_filters) {
        // 简化布隆过滤器配置
        options.OptimizeForPointLookup(config_.range_size);
    }
    
    // 范围索引数据库的特殊配置
    if (is_range_index) {
        options.optimize_filters_for_hits = true;
        options.level_compaction_dynamic_level_bytes = true;
    } else {
        // 数据存储数据库的特殊配置
        options.OptimizeForPointLookup(config_.range_size);
    }
    
    return options;
}

std::string DualRocksDBStrategy::get_range_index_db_path(const std::string& base_path) const {
    return base_path + "_range_index";
}

std::string DualRocksDBStrategy::get_data_storage_db_path(const std::string& base_path) const {
    return base_path + "_data_storage";
}

std::string DualRocksDBStrategy::get_shard_path(const std::string& base_path, size_t shard_index) const {
    return base_path + "_shard_" + std::to_string(shard_index);
}