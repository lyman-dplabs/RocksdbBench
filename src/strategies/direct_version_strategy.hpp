#pragma once
#include "../core/storage_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/table_properties.h>
#include <memory>
#include <sstream>
#include <iomanip>

class DirectVersionStrategy : public IStorageStrategy {
public:
    DirectVersionStrategy() = default;
    virtual ~DirectVersionStrategy() = default;
    
    bool initialize(rocksdb::DB* db) override;
    
    bool write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) override;
    
    std::optional<Value> query_latest_value(rocksdb::DB* db, const std::string& addr_slot) override;
    
    std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                               const std::string& addr_slot, 
                                               BlockNum target_block) override;
    
    std::string get_strategy_name() const override { return "direct_version"; }
    std::string get_description() const override { 
        return "Two-layer storage: version_index(addr_slot:version->block_num) + data(block_num->value)"; 
    }
    
    bool cleanup(rocksdb::DB* db) override;

private:
    bool create_column_families(rocksdb::DB* db);
    
    std::string build_version_key(const std::string& addr_slot, BlockNum version);
    std::string build_data_key(BlockNum block_num, const std::string& addr_slot);
    
    std::optional<BlockNum> find_latest_block_by_version(rocksdb::DB* db, 
                                                         const std::string& version_key,
                                                         const std::string& addr_slot);
    
    std::optional<Value> get_value_by_block(rocksdb::DB* db, BlockNum block_num, const std::string& addr_slot);
};