#pragma once
#include "../core/storage_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/write_batch.h>
#include <memory>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <atomic>
#include <functional>
#include <unordered_map>

class DirectVersionStrategy : public IStorageStrategy {
public:
    // 配置参数
    struct Config {
        uint32_t batch_size_blocks = 5;  // 每个WriteBatch写入的块数（默认5个块）
        size_t max_batch_size_bytes = 4UL * 1024 * 1024 * 1024; // 最大批次大小4GB
    };
    
    DirectVersionStrategy();  // 默认构造函数
    explicit DirectVersionStrategy(const Config& config);
    virtual ~DirectVersionStrategy() = default;
    
    bool initialize(rocksdb::DB* db) override;
    
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    bool write_initial_load_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    
  // 历史版本查询 - 实现复杂语义：≤target_version找最新，找不到则找≥的最小值
  std::optional<Value> query_historical_version(rocksdb::DB* db, 
                                               const std::string& addr_slot, 
                                               BlockNum target_version) override;
    
    std::string get_strategy_name() const override { return "direct_version"; }
    std::string get_description() const override { 
        return "Direct version storage: VERSION|addr_slot:block -> value"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;

private:
    Config config_;
    
    // 保存数据库引用 - 用于flush_all_batches
    rocksdb::DB* db_ref_ = nullptr;
    
    // 批量写入缓存
    mutable std::mutex batch_mutex_;
    mutable rocksdb::WriteBatch pending_batch_;
    mutable size_t current_batch_size_ = 0;
    mutable uint32_t current_batch_blocks_ = 0;
    mutable bool batch_dirty_ = false;
    
    // 统计信息
    std::atomic<uint64_t> total_writes_{0};
    
    // 初始加载批量写入缓存
    mutable rocksdb::WriteBatch pending_batch_initial_;
    
    std::string build_version_key(const std::string& addr_slot, BlockNum version) const;
    
    std::optional<Value> find_value_by_version(rocksdb::DB* db, 
                                                const std::string& version_key,
                                                const std::string& addr_slot);
    
    std::optional<Value> find_value_by_version_with_block(rocksdb::DB* db, 
                                                          const std::string& version_key,
                                                          const std::string& addr_slot);
    std::optional<Value> find_minimum_ge_version(rocksdb::DB* db, 
                                                const std::string& addr_slot, 
                                                BlockNum target_version);
    
    // 批量写入方法
    void add_to_batch(const DataRecord& record);
    bool write_batch_with_processor(rocksdb::DB* db, const std::vector<DataRecord>& records, 
                                   std::function<void(const DataRecord&)> processor);
    
    // 批量写入辅助方法
    bool should_flush_batch(size_t record_size) const;
    void flush_pending_batches(rocksdb::DB* db);
    size_t calculate_block_size(const std::vector<DataRecord>& records) const;
    void add_block_to_pending_batch(const std::vector<DataRecord>& records, size_t block_size);
    
    // 实现接口方法
    void flush_all_batches() override;
};