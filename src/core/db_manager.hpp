#pragma once
#include "types.hpp"
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <memory>
#include <vector>
#include <string>
#include <optional>

class IndexMergeOperator : public rocksdb::MergeOperator {
public:
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
    static std::vector<BlockNum> deserialize_block_list(const std::string& data);
    static std::string serialize_block_list(const std::vector<BlockNum>& blocks);
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

private:
    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::shared_ptr<IndexMergeOperator> merge_operator_;
    bool is_open_ = false;
    
    rocksdb::Options get_db_options();
    std::vector<BlockNum> deserialize_block_list(const std::string& data);
    std::string serialize_block_list(const std::vector<BlockNum>& blocks);
};