#include "direct_version_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/slice.h>
#include <algorithm>

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
        // 1. 写入版本索引: VERSION|address_slot:version -> block_number
        std::string version_key = build_version_key(record.addr_slot, record.block_num);
        std::string block_value = std::to_string(record.block_num);
        batch.Put(version_key, block_value);
        
        // 2. 写入实际数据: DATA|block_number:address_slot -> value
        std::string data_key = build_data_key(record.block_num, record.addr_slot);
        batch.Put(data_key, record.value);
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
    // 1. 构建最大版本key: address_slot:max_version
    std::string max_version_key = build_version_key(addr_slot, UINT64_MAX);
    
    // 2. seek_last找到最新的版本
    auto latest_block_num = find_latest_block_by_version(db, max_version_key, addr_slot);
    if (!latest_block_num) {
        utils::log_debug("No version found for addr_slot: {}", addr_slot.substr(0, 20));
        return std::nullopt;
    }
    
    // 3. 从实际数据表读取value
    return get_value_by_block(db, *latest_block_num, addr_slot);
}

std::optional<Value> DirectVersionStrategy::query_historical_value(rocksdb::DB* db, 
                                                                 const std::string& addr_slot, 
                                                                 BlockNum target_block) {
    // 1. 构建目标版本key: address_slot:target_block
    std::string target_version_key = build_version_key(addr_slot, target_block);
    
    // 2. seek找到<=target_block的最大版本
    auto latest_block_num = find_latest_block_by_version(db, target_version_key, addr_slot);
    if (!latest_block_num) {
        utils::log_debug("No version found for addr_slot: {} target_block: {}", 
                        addr_slot.substr(0, 20), target_block);
        return std::nullopt;
    }
    
    // 3. 从实际数据表读取value
    return get_value_by_block(db, *latest_block_num, addr_slot);
}

std::string DirectVersionStrategy::build_version_key(const std::string& addr_slot, BlockNum version) {
    // 构建版本索引key: VERSION|address_slot:version
    // 使用固定长度格式确保正确的字典序
    std::ostringstream oss;
    oss << "VERSION|" << addr_slot << ":" << std::setw(16) << std::setfill('0') << std::hex << version;
    return oss.str();
}

std::string DirectVersionStrategy::build_data_key(BlockNum block_num, const std::string& addr_slot) {
    // 构建实际数据key: DATA|block_num:address_slot
    std::ostringstream oss;
    oss << "DATA|" << std::setw(8) << std::setfill('0') << std::hex << block_num << "|" << addr_slot;
    return oss.str();
}

std::optional<BlockNum> DirectVersionStrategy::find_latest_block_by_version(rocksdb::DB* db, 
                                                                         const std::string& version_key,
                                                                         const std::string& addr_slot) {
    // 使用RocksDB的Seek功能找到<=version_key的最大版本
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
            // 找到了匹配的key，解析block_number
            try {
                return std::stoull(it->value().ToString());
            } catch (const std::exception& e) {
                utils::log_error("Failed to parse block number from value: {}", it->value().ToString());
                break;
            }
        }
        
        // 如果key已经小于VERSION|addr_slot，说明没有找到
        if (current_key_str < expected_prefix) {
            break;
        }
        
        it->Prev();
    }
    
    return std::nullopt;
}

std::optional<Value> DirectVersionStrategy::get_value_by_block(rocksdb::DB* db, BlockNum block_num, const std::string& addr_slot) {
    // 从实际数据表读取value
    std::string data_key = build_data_key(block_num, addr_slot);
    
    std::string value;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), data_key, &value);
    
    if (status.ok()) {
        return value;
    } else {
        utils::log_debug("Value not found for block {} addr_slot {}", block_num, addr_slot.substr(0, 20));
        return std::nullopt;
    }
}

bool DirectVersionStrategy::cleanup(rocksdb::DB* db) {
    // 简化实现：不需要清理列族句柄
    utils::log_info("DirectVersionStrategy cleanup completed");
    return true;
}