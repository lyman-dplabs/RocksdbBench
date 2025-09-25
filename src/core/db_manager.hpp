#pragma once
#include "types.hpp"
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <rocksdb/statistics.h>
#include <memory>
#include <vector>
#include <string>
#include <optional>
#include <functional>

class IndexMergeOperator : public rocksdb::MergeOperator {
public:
    explicit IndexMergeOperator(std::function<void(size_t, size_t)> merge_callback = nullptr)
        : merge_callback_(merge_callback) {}
    
    virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                            MergeOperationOutput* merge_out) const override;
    
    virtual bool PartialMergeMulti(const rocksdb::Slice& key,
                                  const std::deque<rocksdb::Slice>& operand_list,
                                  std::string* new_value,
                                  rocksdb::Logger* logger) const override;
    
    virtual const char* Name() const override {
        return "IndexMergeOperator";
    }
    
private:
    static std::vector<BlockNum> merge_deserialize_block_list(const std::string& data);
    static std::string merge_serialize_block_list(const std::vector<BlockNum>& blocks);
    std::function<void(size_t, size_t)> merge_callback_;
};

class DBManager {
public:
    explicit DBManager(const std::string& db_path);
    ~DBManager();

    DBManager(const DBManager&) = delete;
    DBManager& operator=(const DBManager&) = delete;

    bool write_batch(const std::vector<ChangeSetRecord>& changes, const std::vector<IndexRecord>& indices);
    std::optional<Value> get_historical_state(const std::string& addr_slot, BlockNum target_block_num);
    bool open(bool force_clean = false);
    void close();
    bool data_exists() const;
    bool clean_data();
    
    // Set metrics callback for merge operations
    void set_merge_callback(std::function<void(size_t, size_t)> callback) {
        merge_callback_ = callback;
    }
    
    // Get Bloom filter statistics from RocksDB
    uint64_t get_bloom_filter_hits() const;
    uint64_t get_bloom_filter_misses() const;
    uint64_t get_point_query_total() const;
    
    // Get compaction statistics
    uint64_t get_compaction_bytes_read() const;
    uint64_t get_compaction_bytes_written() const;
    uint64_t get_compaction_time_micros() const;
    
    // Debug function to check bloom filter statistics
    void debug_bloom_filter_stats() const;

private:
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::shared_ptr<IndexMergeOperator> merge_operator_;
    std::shared_ptr<rocksdb::Statistics> statistics_;
    std::function<void(size_t, size_t)> merge_callback_;
    bool is_open_ = false;
    
    rocksdb::Options get_db_options();
    std::vector<BlockNum> deserialize_block_list(const std::string& data);
    std::string serialize_block_list(const std::vector<BlockNum>& blocks);
};