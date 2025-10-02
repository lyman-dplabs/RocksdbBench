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
    
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    
    std::string get_strategy_name() const override { return "direct_version"; }
    std::string get_description() const override { 
        return "Direct version storage: VERSION|addr_slot:block -> value"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;

private:
    Config config_;
    
    // 批量写入缓存
    mutable std::mutex batch_mutex_;
    mutable rocksdb::WriteBatch pending_batch_;
    mutable size_t current_batch_size_ = 0;
    mutable uint32_t current_batch_blocks_ = 0;
    mutable bool batch_dirty_ = false;
    
    // 统计信息
    std::atomic<uint64_t> total_writes_{0};
    
    bool create_column_families(rocksdb::DB* db);
    
    std::string build_version_key(const std::string& addr_slot, BlockNum version);
    
    std::optional<Value> find_value_by_version(rocksdb::DB* db, 
                                                const std::string& version_key,
                                                const std::string& addr_slot);
    
    // 批量写入方法
    void add_to_batch(const DataRecord& record);
    bool write_batch_with_processor(rocksdb::DB* db, const std::vector<DataRecord>& records, 
                                   std::function<void(const DataRecord&)> processor);
};