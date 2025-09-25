#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <rocksdb/db.h>

using BlockNum = uint64_t;
using Value = std::string;

// 统一的数据记录格式
struct DataRecord {
    BlockNum block_num;
    std::string addr_slot;
    Value value;
};

// 存储策略接口 - 每个策略完全独立管理自己的数据结构
class IStorageStrategy {
public:
    virtual ~IStorageStrategy() = default;
    
    // 初始化存储结构（创建表、设置选项等）
    virtual bool initialize(rocksdb::DB* db) = 0;
    
    // 写入数据 - 支持不同的数据格式
    virtual bool write_batch(rocksdb::DB* db, 
                           const std::vector<DataRecord>& records) = 0;
    
    // 查询最新值
    virtual std::optional<Value> query_latest_value(rocksdb::DB* db, 
                                                   const std::string& addr_slot) = 0;
    
    // 查询历史值
    virtual std::optional<Value> query_historical_value(rocksdb::DB* db, 
                                                       const std::string& addr_slot, 
                                                       BlockNum target_block) = 0;
    
    // 策略信息
    virtual std::string get_strategy_name() const = 0;
    virtual std::string get_description() const = 0;
    
    // 清理数据
    virtual bool cleanup(rocksdb::DB* db) = 0;
};