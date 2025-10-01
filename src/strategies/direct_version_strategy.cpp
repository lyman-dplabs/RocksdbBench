#include "direct_version_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include <algorithm>
#include <iomanip>
#include <sstream>

bool DirectVersionStrategy::create_column_families(rocksdb::DB* db) {
    // 简化实现：不使用列族，而是使用键前缀来区分不同类型的数据
    utils::log_info("DirectVersionStrategy initialized - using key prefixes instead of column families");
    return true;
}

bool DirectVersionStrategy::initialize(rocksdb::DB* db) {
    return create_column_families(db);
}

bool DirectVersionStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    rocksdb::WriteBatch batch;
    
    for (const auto& record : records) {
        // 直接存储: VERSION|addr_slot:block_number -> value
        std::string version_key = build_version_key(record.addr_slot, record.block_num);
        batch.Put(version_key, record.value);
    }
    
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    auto status = db->Write(write_options, &batch);
    
    if (!status.ok()) {
        utils::log_error("Failed to write batch: {}", status.ToString());
        return false;
    }
    
    return true;
}

std::optional<Value> DirectVersionStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    // 构建最大版本key用于seek
    std::string max_version_key = build_version_key(addr_slot, UINT64_MAX);
    
    // 直接查找<=max_version_key的最大版本，返回value
    return find_value_by_version(db, max_version_key, addr_slot);
}

std::optional<Value> DirectVersionStrategy::query_historical_value(rocksdb::DB* db, 
                                                                 const std::string& addr_slot, 
                                                                 BlockNum target_block) {
    // 构建目标版本key用于seek
    std::string target_version_key = build_version_key(addr_slot, target_block);
    
    // 直接查找<=target_block的最大版本，返回value
    return find_value_by_version(db, target_version_key, addr_slot);
}

std::string DirectVersionStrategy::build_version_key(const std::string& addr_slot, BlockNum version) {
    // 构建版本索引key: VERSION|address_slot:version
    // 使用固定长度格式确保正确的字典序
    std::ostringstream oss;
    oss << "VERSION|" << addr_slot << ":" << std::setw(16) << std::setfill('0') << std::hex << version;
    return oss.str();
}


std::optional<Value> DirectVersionStrategy::find_value_by_version(rocksdb::DB* db, 
                                                                const std::string& version_key,
                                                                const std::string& addr_slot) {
    // 使用RocksDB的Seek功能找到<=version_key的最大版本，直接返回value
    rocksdb::ReadOptions read_options;
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options));
    
    it->Seek(version_key);
    
    // 如果seek超出了范围，从最后一个开始
    if (!it->Valid()) {
        it->SeekToLast();
    }
    
    while (it->Valid()) {
        rocksdb::Slice current_key = it->key();
        std::string current_key_str = current_key.ToString();
        
        // 检查key是否以VERSION|addr_slot:开头
        std::string expected_prefix = "VERSION|" + addr_slot + ":";
        if (current_key_str.find(expected_prefix) == 0) {
            // 找到了匹配的key，直接返回value
            return it->value().ToString();
        }
        
        // 如果key已经小于VERSION|addr_slot，说明没有找到
        if (current_key_str < expected_prefix) {
            break;
        }
        
        it->Prev();
    }
    
    return std::nullopt;
}

bool DirectVersionStrategy::cleanup(rocksdb::DB* db) {
    // 简化实现：不需要清理列族句柄
    utils::log_info("DirectVersionStrategy cleanup completed");
    return true;
}