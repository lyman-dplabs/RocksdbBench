#include "page_index_strategy.hpp"
#include "../utils/logger.hpp"
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <algorithm>
#include <sstream>

PageIndexStrategy::PageIndexStrategy(std::function<void(size_t, size_t)> merge_callback)
    : merge_callback_(merge_callback) {
    merge_operator_ = std::make_shared<PageIndexMergeOperator>(merge_callback);
}

bool PageIndexStrategy::initialize(rocksdb::DB* db) {
    // For PageIndexStrategy, we use the default column family
    // The existing logic works with the default RocksDB setup
    utils::log_info("PageIndexStrategy initialized - using default column family");
    return true;
}

bool PageIndexStrategy::write_batch(rocksdb::DB* db, const std::vector<DataRecord>& records) {
    // Convert DataRecord format to ChangeSet and Index format
    std::vector<ChangeSetRecord> changes;
    std::vector<IndexRecord> indices;
    
    changes.reserve(records.size());
    indices.reserve(records.size());
    
    for (const auto& record : records) {
        ChangeSetRecord change{record.block_num, record.addr_slot, record.value};
        changes.push_back(change);
        
        PageNum page = block_to_page(record.block_num);
        IndexRecord index{page, record.addr_slot, {record.block_num}};
        indices.push_back(index);
    }
    
    return write_batch_internal(db, changes, indices);
}

std::optional<Value> PageIndexStrategy::query_latest_value(rocksdb::DB* db, const std::string& addr_slot) {
    // Use the existing logic: find_latest_block_for_key + get_historical_state
    auto latest_block = find_latest_block_for_key(db, addr_slot, UINT64_MAX);
    if (!latest_block) {
        utils::log_debug("No block found for addr_slot: {}", addr_slot.substr(0, 20));
        return std::nullopt;
    }
    return get_historical_state(db, addr_slot, *latest_block);
}

std::optional<Value> PageIndexStrategy::query_historical_value(rocksdb::DB* db, 
                                                              const std::string& addr_slot, 
                                                              BlockNum target_block) {
    // Direct use of existing logic
    return get_historical_state(db, addr_slot, target_block);
}

bool PageIndexStrategy::cleanup(rocksdb::DB* db) {
    // No special cleanup needed for PageIndexStrategy
    return true;
}

bool PageIndexStrategy::write_batch_internal(rocksdb::DB* db, 
                                            const std::vector<ChangeSetRecord>& changes, 
                                            const std::vector<IndexRecord>& indices) {
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.sync = false;

    // Write ChangeSet records
    for (const auto& change : changes) {
        batch.Put(change.to_key(), change.value);
    }

    // Write Index records with merge operator
    for (const auto& index : indices) {
        std::string serialized_blocks = serialize_block_list(index.block_history);
        batch.Merge(index.to_key(), serialized_blocks);
    }

    rocksdb::Status status = db->Write(write_options, &batch);
    return status.ok();
}

std::optional<BlockNum> PageIndexStrategy::find_latest_block_for_key(rocksdb::DB* db, 
                                                                     const std::string& addr_slot, 
                                                                     BlockNum max_known_block) const {
    // Calculate the maximum page number from the known maximum block
    PageNum max_page = block_to_page(max_known_block) + 10; // Add some buffer
    
    BlockNum latest_block = 0;
    bool found = false;
    
    // Search from the newest page backwards for efficiency
    for (PageNum page = max_page; page >= 0 && page <= max_page; --page) {
        IndexRecord index_query{page, addr_slot, {}};
        std::string index_data;
        
        rocksdb::Status status = db->Get(rocksdb::ReadOptions(), index_query.to_key(), &index_data);
        
        if (status.ok()) {
            // Found the key in this page, deserialize block list
            std::vector<BlockNum> block_list = deserialize_block_list(index_data);
            if (!block_list.empty()) {
                // Get the latest (maximum) block number for this key
                BlockNum page_latest = *std::max_element(block_list.begin(), block_list.end());
                if (page_latest <= max_known_block && page_latest > latest_block) {
                    latest_block = page_latest;
                    found = true;
                }
            }
        }
    }
    
    return found ? std::optional<BlockNum>(latest_block) : std::nullopt;
}

std::optional<Value> PageIndexStrategy::get_historical_state(rocksdb::DB* db, 
                                                               const std::string& addr_slot, 
                                                               BlockNum target_block_num) {
    PageNum target_page = block_to_page(target_block_num);
    IndexRecord index_query{target_page, addr_slot, {}};
    
    std::string index_data;
    rocksdb::Status status = db->Get(rocksdb::ReadOptions(), index_query.to_key(), &index_data);
    
    if (!status.ok()) {
        utils::log_debug("Index not found for page {} addr_slot {}", target_page, addr_slot.substr(0, 20));
        return std::nullopt;
    }
    
    std::vector<BlockNum> block_list = deserialize_block_list(index_data);
    
    if (block_list.empty()) {
        utils::log_debug("Empty block list for page {} addr_slot {}", target_page, addr_slot.substr(0, 20));
        return std::nullopt;
    }
    
    auto it = std::upper_bound(block_list.begin(), block_list.end(), target_block_num);
    if (it == block_list.begin()) {
        utils::log_debug("No block found <= {} for addr_slot {}. Available blocks: {}", 
                        target_block_num, addr_slot.substr(0, 20), block_list.size());
        return std::nullopt;
    }
    
    BlockNum closest_block = *(--it);
    ChangeSetRecord changeset_query{closest_block, addr_slot, ""};
    
    std::string value;
    status = db->Get(rocksdb::ReadOptions(), changeset_query.to_key(), &value);
    
    if (status.ok()) {
        return value;
    } else {
        utils::log_debug("Value not found for block {} addr_slot {}", closest_block, addr_slot.substr(0, 20));
        return std::nullopt;
    }
}

std::vector<BlockNum> PageIndexStrategy::deserialize_block_list(const std::string& data) const {
    std::vector<BlockNum> blocks;
    if (data.empty() || data.size() % sizeof(BlockNum) != 0) {
        return blocks;
    }
    
    const BlockNum* ptr = reinterpret_cast<const BlockNum*>(data.data());
    size_t count = data.size() / sizeof(BlockNum);
    
    blocks.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        blocks.push_back(ptr[i]);
    }
    
    return blocks;
}

std::string PageIndexStrategy::serialize_block_list(const std::vector<BlockNum>& blocks) {
    std::string result;
    result.resize(blocks.size() * sizeof(BlockNum));
    
    BlockNum* ptr = reinterpret_cast<BlockNum*>(result.data());
    for (size_t i = 0; i < blocks.size(); ++i) {
        ptr[i] = blocks[i];
    }
    
    return result;
}

// MergeOperator implementation
bool PageIndexMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                        MergeOperationOutput* merge_out) const {
    if (merge_in.existing_value == nullptr && merge_in.operand_list.empty()) {
        return false;
    }
    
    std::vector<BlockNum> blocks;
    
    // Parse existing value
    if (merge_in.existing_value != nullptr) {
        blocks = merge_deserialize_block_list(merge_in.existing_value->ToString());
    }
    
    // Merge all operands
    for (const auto& operand : merge_in.operand_list) {
        std::vector<BlockNum> operand_blocks = merge_deserialize_block_list(operand.ToString());
        blocks.insert(blocks.end(), operand_blocks.begin(), operand_blocks.end());
    }
    
    // Remove duplicates and sort
    std::sort(blocks.begin(), blocks.end());
    blocks.erase(std::unique(blocks.begin(), blocks.end()), blocks.end());
    
    // Serialize result
    merge_out->new_value = merge_serialize_block_list(blocks);
    
    // Call merge callback if set
    if (merge_callback_) {
        merge_callback_(blocks.size(), merge_out->new_value.size());
    }
    
    return true;
}

bool PageIndexMergeOperator::PartialMergeMulti(const rocksdb::Slice& key,
                                                const std::deque<rocksdb::Slice>& operand_list,
                                                std::string* new_value,
                                                rocksdb::Logger* logger) const {
    std::vector<BlockNum> all_blocks;
    
    // Collect all blocks from all operands
    for (const auto& operand : operand_list) {
        std::vector<BlockNum> blocks = merge_deserialize_block_list(operand.ToString());
        all_blocks.insert(all_blocks.end(), blocks.begin(), blocks.end());
    }
    
    // Remove duplicates and sort
    std::sort(all_blocks.begin(), all_blocks.end());
    all_blocks.erase(std::unique(all_blocks.begin(), all_blocks.end()), all_blocks.end());
    
    // Serialize result
    *new_value = merge_serialize_block_list(all_blocks);
    
    return true;
}

std::vector<BlockNum> PageIndexMergeOperator::merge_deserialize_block_list(const std::string& data) {
    std::vector<BlockNum> blocks;
    if (data.empty() || data.size() % sizeof(BlockNum) != 0) {
        return blocks;
    }
    
    const BlockNum* ptr = reinterpret_cast<const BlockNum*>(data.data());
    size_t count = data.size() / sizeof(BlockNum);
    
    blocks.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        blocks.push_back(ptr[i]);
    }
    
    return blocks;
}

std::string PageIndexMergeOperator::merge_serialize_block_list(const std::vector<BlockNum>& blocks) {
    std::string result;
    result.resize(blocks.size() * sizeof(BlockNum));
    
    BlockNum* ptr = reinterpret_cast<BlockNum*>(result.data());
    for (size_t i = 0; i < blocks.size(); ++i) {
        ptr[i] = blocks[i];
    }
    
    return result;
}