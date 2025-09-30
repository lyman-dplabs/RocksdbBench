#pragma once
#include "storage_strategy.hpp"
#include "types.hpp"
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>
#include <memory>
#include <string>

class StrategyDBManager {
public:
    explicit StrategyDBManager(const std::string& db_path, 
                             std::unique_ptr<IStorageStrategy> strategy);
    ~StrategyDBManager();

    StrategyDBManager(const StrategyDBManager&) = delete;
    StrategyDBManager& operator=(const StrategyDBManager&) = delete;

    bool open(bool force_clean = false);
    void close();
    bool data_exists() const;
    bool clean_data();
    
    // 新的统一接口
    bool write_batch(const std::vector<DataRecord>& records);
    std::optional<Value> query_latest_value(const std::string& addr_slot);
    std::optional<Value> query_historical_value(const std::string& addr_slot, BlockNum target_block);
    
    // 兼容现有接口 - 用于PageIndexStrategy
    bool write_batch(const std::vector<ChangeSetRecord>& changes, const std::vector<IndexRecord>& indices);
    std::optional<Value> get_historical_state(const std::string& addr_slot, BlockNum target_block_num);
    
    // 获取当前策略信息
    std::string get_strategy_name() const { return strategy_->get_strategy_name(); }
    std::string get_strategy_description() const { return strategy_->get_description(); }
    
    // 批量写入模式控制（仅适用于支持批量写入的策略）
    void set_batch_mode(bool enable);
    
    // Get RocksDB statistics
    uint64_t get_bloom_filter_hits() const;
    uint64_t get_bloom_filter_misses() const;
    uint64_t get_point_query_total() const;
    uint64_t get_compaction_bytes_read() const;
    uint64_t get_compaction_bytes_written() const;
    uint64_t get_compaction_time_micros() const;
    void debug_bloom_filter_stats() const;
    
    // Bloom filter configuration
    void set_bloom_filter_enabled(bool enabled);
    
    // Merge callback for PageIndexStrategy
    void set_merge_callback(std::function<void(size_t, size_t)> callback);
    
    // Statistics structures
    struct BloomFilterStats {
        uint64_t hits = 0;
        uint64_t misses = 0;
        uint64_t total_queries = 0;
    };
    
    struct CompactionStats {
        uint64_t bytes_read = 0;
        uint64_t bytes_written = 0;
        uint64_t time_micros = 0;
    };
    
    BloomFilterStats get_bloom_filter_stats() const;
    CompactionStats get_compaction_stats() const;

private:
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::unique_ptr<IStorageStrategy> strategy_;
    std::shared_ptr<rocksdb::Statistics> statistics_;
    bool is_open_ = false;
    
    rocksdb::Options get_db_options();
};