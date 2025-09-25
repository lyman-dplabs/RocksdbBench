#pragma once
#include "../core/storage_strategy.hpp"
#include "../core/types.hpp"
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/statistics.h>
#include <memory>
#include <vector>
#include <string>
#include <optional>
#include <functional>

class PageIndexMergeOperator : public rocksdb::MergeOperator {
public:
    explicit PageIndexMergeOperator(std::function<void(size_t, size_t)> merge_callback = nullptr)
        : merge_callback_(merge_callback) {}
    
    virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                            MergeOperationOutput* merge_out) const override;
    
    virtual bool PartialMergeMulti(const rocksdb::Slice& key,
                                  const std::deque<rocksdb::Slice>& operand_list,
                                  std::string* new_value,
                                  rocksdb::Logger* logger) const override;
    
    virtual const char* Name() const override {
        return "PageIndexMergeOperator";
    }
    
private:
    static std::vector<BlockNum> merge_deserialize_block_list(const std::string& data);
    static std::string merge_serialize_block_list(const std::vector<BlockNum>& blocks);
    std::function<void(size_t, size_t)> merge_callback_;
};

class PageIndexStrategy : public IStorageStrategy {
public:
    explicit PageIndexStrategy(std::function<void(size_t, size_t)> merge_callback = nullptr);
    
    bool initialize(rocksdb::DB* db) override;
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                               const std::string& addr_slot, 
                                               BlockNum target_block) override;
    
    std::string get_strategy_name() const override { return "page_index"; }
    std::string get_description() const override { 
        return "Traditional ChangeSet + Index tables with page-based organization"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;
    
    // Set metrics callback for merge operations
    void set_merge_callback(std::function<void(size_t, size_t)> callback) {
        merge_callback_ = callback;
    }
    
    // 兼容原有接口
    bool write_batch_internal(rocksdb::DB* db, 
                             const std::vector<ChangeSetRecord>& changes, 
                             const std::vector<IndexRecord>& indices);

private:
    std::shared_ptr<PageIndexMergeOperator> merge_operator_;
    std::function<void(size_t, size_t)> merge_callback_;
    
    std::optional<BlockNum> find_latest_block_for_key(rocksdb::DB* db, 
                                                       const std::string& addr_slot, 
                                                       BlockNum max_known_block) const;
    std::optional<Value> get_historical_state(rocksdb::DB* db, 
                                              const std::string& addr_slot, 
                                              BlockNum target_block_num);
    
    std::vector<BlockNum> deserialize_block_list(const std::string& data) const;
    std::string serialize_block_list(const std::vector<BlockNum>& blocks);
    
    rocksdb::Options get_db_options();
};