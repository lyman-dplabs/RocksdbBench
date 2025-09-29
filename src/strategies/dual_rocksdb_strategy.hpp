#pragma once
#include "../core/storage_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/db.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <chrono>
#include <mutex>
#include <atomic>

using BlockNum = uint64_t;
using Value = std::string;

// 缓存级别枚举
enum class CacheLevel {
    HOT,        // 热点数据：完整数据缓存
    MEDIUM,     // 中等数据：范围列表缓存
    PASSIVE     // 被动缓存：仅查询时缓存
};

// 访问统计信息
struct AccessStats {
    size_t access_count = 0;
    std::chrono::system_clock::time_point last_access;
    std::chrono::system_clock::time_point first_access;
};

// 缓存条目
struct CacheEntry {
    Value value;
    std::chrono::system_clock::time_point last_access;
    std::chrono::system_clock::time_point created;
};

// 自适应缓存管理器
class AdaptiveCacheManager {
private:
    // L1缓存：热点数据完整缓存
    std::unordered_map<std::string, CacheEntry> hot_cache_;
    
    // L2缓存：中等数据范围列表缓存
    std::unordered_map<std::string, std::vector<uint32_t>> range_cache_;
    
    // L3缓存：被动查询缓存
    std::unordered_map<std::string, Value> passive_cache_;
    
    // 访问统计
    std::unordered_map<std::string, AccessStats> access_stats_;
    
    // 内存监控
    size_t current_memory_usage_ = 0;
    size_t max_memory_limit_;
    mutable std::mutex cache_mutex_;
    
    // 配置参数
    double hot_cache_ratio_ = 0.01;
    double medium_cache_ratio_ = 0.05;
    bool enable_memory_monitor_ = true;
    
public:
    explicit AdaptiveCacheManager(size_t max_memory_bytes = 1024 * 1024 * 1024);
    
    // 缓存操作
    void cache_hot_data(const std::string& key, const Value& value);
    void cache_range_list(const std::string& key, const std::vector<uint32_t>& ranges);
    void cache_passive_data(const std::string& key, const Value& value);
    
    // 缓存查询
    std::optional<Value> get_hot_data(const std::string& key);
    std::optional<std::vector<uint32_t>> get_range_list(const std::string& key);
    std::optional<Value> get_passive_data(const std::string& key);
    
    // 访问模式更新
    void update_access_pattern(const std::string& key, bool is_write);
    CacheLevel determine_cache_level(const std::string& key) const;
    
    // 内存管理
    size_t get_memory_usage() const;
    void manage_memory_pressure();
    void evict_least_used();
    
    // 配置更新
    void set_config(double hot_ratio, double medium_ratio);
    
    // 清理操作
    void clear_expired();
    void clear_all();
    
private:
    bool has_memory_for_hot_data() const;
    bool has_memory_for_medium_data() const;
    void update_memory_usage(size_t added_bytes);
    size_t estimate_entry_size(const Value& value) const;
};

// 双RocksDB范围分区存储策略
class DualRocksDBStrategy : public IStorageStrategy {
private:
    // 双RocksDB实例
    std::unique_ptr<rocksdb::DB> range_index_db_;
    std::unique_ptr<rocksdb::DB> data_storage_db_;
    
    // 自适应缓存管理器
    std::unique_ptr<AdaptiveCacheManager> cache_manager_;
    
  public:
    // 配置参数
    struct Config {
        uint32_t range_size = 10000;
        size_t max_cache_memory = 1024 * 1024 * 1024; // 1GB默认
        double hot_cache_ratio = 0.01;
        double medium_cache_ratio = 0.05;
        bool enable_compression = true;
        bool enable_bloom_filters = true;
        bool enable_dynamic_cache_optimization = false;
        uint64_t expected_key_count = 0;
        bool enable_sharding = false;
        size_t shard_count = 1;
    };
    
private:
    Config config_;
    
    // 统计信息
    std::atomic<uint64_t> total_reads_{0};
    std::atomic<uint64_t> total_writes_{0};
    std::atomic<uint64_t> cache_hits_{0};
    
public:
    explicit DualRocksDBStrategy(const Config& config);
    ~DualRocksDBStrategy();
    
    // IStorageStrategy 接口实现
    bool initialize(rocksdb::DB* main_db) override;
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                               const std::string& addr_slot, 
                                               BlockNum target_block) override;
    
    std::string get_strategy_name() const override { return "dual_rocksdb_adaptive"; }
    std::string get_description() const override { 
        return "双RocksDB范围分区存储，具有自适应内存管理"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;
    
    // 配置接口
    void set_config(const Config& config);
    const Config& get_config() const { return config_; }
    
    // 统计接口
    uint64_t get_total_reads() const { return total_reads_.load(); }
    uint64_t get_total_writes() const { return total_writes_.load(); }
    uint64_t get_cache_hits() const { return cache_hits_.load(); }
    double get_cache_hit_rate() const;
    
    // 分片接口（可选功能）
    std::string get_shard_path(const std::string& base_path, size_t shard_index) const;

private:
    // 核心操作
    uint32_t calculate_range(BlockNum block_num) const;
    std::string build_data_key(uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const;
    std::string build_data_prefix(uint32_t range_num, const std::string& addr_slot) const;
    
    // Seek-Last查找优化（核心机制，强制启用）
    std::optional<BlockNum> find_latest_block_in_range(rocksdb::DB* db, 
                                                         uint32_t range_num, 
                                                         const std::string& addr_slot) const;
    
    // 范围管理
    bool update_range_index(rocksdb::DB* db, const std::string& addr_slot, uint32_t range_num);
    std::vector<uint32_t> get_address_ranges(rocksdb::DB* db, const std::string& addr_slot) const;
    
    // 双DB操作
    bool open_databases(const std::string& base_path);
    bool create_range_index_db(const std::string& path);
    bool create_data_storage_db(const std::string& path);
    
    // 辅助方法
    std::optional<Value> get_value_from_data_db(rocksdb::DB* db, 
                                                uint32_t range_num, 
                                                const std::string& addr_slot, 
                                                BlockNum block_num);
    
    BlockNum extract_block_from_key(const std::string& key) const;
    std::vector<uint32_t> deserialize_range_list(const std::string& data) const;
    std::string serialize_range_list(const std::vector<uint32_t>& ranges) const;
    
    // 内存管理
    void check_memory_pressure();
    void optimize_cache_usage();
    
    // 工具方法
    rocksdb::Options get_rocksdb_options(bool is_range_index = false) const;
    std::string get_range_index_db_path(const std::string& base_path) const;
    std::string get_data_storage_db_path(const std::string& base_path) const;
    
    // 动态缓存优化（可选功能）
    void dynamic_cache_optimization();
    void reclassify_cache_entries();
};