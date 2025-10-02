#pragma once
#include "../core/storage_strategy.hpp"
#include "../utils/logger.hpp"
#include "dual_rocksdb_cache_manager.hpp"
#include <rocksdb/db.h>
#include <memory>
#include <unordered_map>
#include <vector>
#include <string>
#include <optional>
#include <chrono>
#include <mutex>
#include <atomic>
#include <functional>

using BlockNum = uint64_t;
using Value = std::string;

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
        
        // 批量写入配置
        uint32_t batch_size_blocks = 5;  // 每个WriteBatch写入的块数（默认5个块）
        size_t max_batch_size_bytes = 128 * 1024 * 1024; // 最大批次大小128MB
    };
    
private:
    Config config_;
    
    // 统计信息
    std::atomic<uint64_t> total_reads_{0};
    std::atomic<uint64_t> total_writes_{0};
    std::atomic<uint64_t> cache_hits_{0};
    
    // 复用DBManager的SST合并效率统计
    // 通过主数据库的statistics_获取compaction指标
    
    // 批量写入缓存
    mutable std::mutex batch_mutex_;
    mutable rocksdb::WriteBatch pending_range_batch_;
    mutable rocksdb::WriteBatch pending_data_batch_;
    mutable size_t current_batch_size_ = 0;
    mutable uint32_t current_batch_blocks_ = 0;
    mutable bool batch_dirty_ = false;
    
    // 批量写入期间的range索引缓存，避免重复查询
    mutable std::unordered_map<std::string, std::vector<uint32_t>> batch_range_cache_;
    
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
    
    // Initial Load专用接口 - 优化首次导入性能
    bool write_initial_load_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    
    std::string get_strategy_name() const override { return "dual_rocksdb_adaptive"; }
    std::string get_description() const override { 
        return "双RocksDB范围分区存储，具有自适应内存管理"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;
    
    // 配置接口
    void set_config(const Config& config);
    const Config& get_config() const { return config_; }
    
    // 批量写入接口
    void flush_all_batches() override;  // 强制刷写所有待写入批次
    
    // 统计接口
    uint64_t get_total_reads() const { return total_reads_.load(); }
    uint64_t get_total_writes() const { return total_writes_.load(); }
    uint64_t get_cache_hits() const { return cache_hits_.load(); }
    double get_cache_hit_rate() const;
    
    // SST合并效率统计接口 - 复用DBManager的统计
    uint64_t get_compaction_bytes_written() const;
    uint64_t get_compaction_bytes_read() const;
    uint64_t get_compaction_count() const;
    double get_compaction_efficiency() const;
    
    // 分片接口（可选功能）
    std::string get_shard_path(const std::string& base_path, size_t shard_index) const;

private:
    // 核心操作
    uint32_t calculate_range(BlockNum block_num) const;
    std::string build_data_key(uint32_t range_num, const std::string& addr_slot, BlockNum block_num) const;
    std::string build_data_prefix(uint32_t range_num, const std::string& addr_slot) const;
    
        
    // Seek-Last查找优化（核心机制，强制启用）
    std::optional<Value> find_latest_block_in_range(rocksdb::DB* db, 
                                                     uint32_t range_num, 
                                                     const std::string& addr_slot, 
                                                     BlockNum max_block = UINT64_MAX) const;
    
    // 范围管理
    bool update_range_index(rocksdb::DB* db, const std::string& addr_slot, uint32_t range_num);
    std::vector<uint32_t> get_address_ranges(rocksdb::DB* db, const std::string& addr_slot) const;
    
    // 双DB操作
    bool open_databases(const std::string& base_path);
    bool create_range_index_db(const std::string& path);
    bool create_data_storage_db(const std::string& path);
    
    // 辅助方法
    BlockNum extract_block_from_key(const std::string& key) const;
    std::vector<uint32_t> deserialize_range_list(const std::string& data) const;
    std::string serialize_range_list(const std::vector<uint32_t>& ranges) const;
    
    // 内存管理
    void check_memory_pressure();
    void optimize_cache_usage();
    
    // 批量写入管理
    void flush_pending_batches();
    bool should_flush_batch(size_t record_size) const;
    
    // 批量写入通用方法
    void process_record_for_batch(const DataRecord& record, rocksdb::WriteBatch& range_batch, rocksdb::WriteBatch& data_batch, bool is_initial_load);
    bool execute_batch_write(rocksdb::WriteBatch& range_batch, rocksdb::WriteBatch& data_batch, const char* operation_name);
    
    // 重构后的写入辅助方法
    struct RangeIndexUpdates {
        std::unordered_map<std::string, std::vector<uint32_t>> ranges_to_update;
    };
    
    RangeIndexUpdates collect_range_updates_for_hotspot(const std::vector<DataRecord>& records);
    void add_data_to_batch(const std::vector<DataRecord>& records, rocksdb::WriteBatch& data_batch);
    void build_range_index_batch(const RangeIndexUpdates& updates, rocksdb::WriteBatch& range_batch);
    size_t calculate_block_size(const std::vector<DataRecord>& records) const;
    void add_block_to_pending_batch(const std::vector<DataRecord>& records, size_t block_size);
    
    // 工具方法
    rocksdb::Options get_rocksdb_options(bool is_range_index = false) const;
    std::string get_range_index_db_path(const std::string& base_path) const;
    std::string get_data_storage_db_path(const std::string& base_path) const;
    
    // 动态缓存优化（可选功能）
    void dynamic_cache_optimization();
    void reclassify_cache_entries();
};