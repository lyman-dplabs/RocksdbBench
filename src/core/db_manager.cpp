#include "db_manager.hpp"
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sys/stat.h>
#include "../utils/logger.hpp"

DBManager::DBManager(const std::string& db_path) 
    : db_path_(db_path), merge_operator_(std::make_shared<IndexMergeOperator>([this](size_t merged_values, size_t merged_value_size) {
        if (merge_callback_) {
            merge_callback_(merged_values, merged_value_size);
        }
    })) {}

DBManager::~DBManager() {
    close();
}

bool DBManager::open(bool force_clean) {
    try {
        if (std::filesystem::exists(db_path_)) {
            if (force_clean) {
                if (!clean_data()) {
                    return false;
                }
            } else {
                return false;
            }
        }
        
        if (!std::filesystem::exists(db_path_)) {
            std::filesystem::create_directories(db_path_);
        }
    } catch (const std::exception& e) {
        utils::log_error("Failed to create directory {}: {}", db_path_, e.what());
        return false;
    }
    
    rocksdb::Options options = get_db_options();
    
    rocksdb::Status status = rocksdb::DB::Open(options, db_path_, &db_);
    if (status.ok()) {
        is_open_ = true;
        return true;
    } else {
        utils::log_error("Failed to open RocksDB: {}", status.ToString());
        return false;
    }
}

void DBManager::close() {
    if (is_open_ && db_) {
        db_->Close();
        is_open_ = false;
    }
}

bool DBManager::data_exists() const {
    return std::filesystem::exists(db_path_) && 
           (std::filesystem::exists(db_path_ + "/CURRENT") || 
            std::filesystem::exists(db_path_ + "/MANIFEST-000000"));
}

bool DBManager::clean_data() {
    try {
        if (std::filesystem::exists(db_path_)) {
            std::uintmax_t removed = std::filesystem::remove_all(db_path_);
            utils::log_info("Removed existing data directory: {} ({} files)", db_path_, removed);
            return true;
        }
        return true;
    } catch (const std::exception& e) {
        utils::log_error("Failed to clean data directory {}: {}", db_path_, e.what());
        return false;
    }
}

rocksdb::Options DBManager::get_db_options() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator = merge_operator_;
    options.compression = rocksdb::kNoCompression;
    options.max_open_files = -1;
    options.use_fsync = false;
    options.stats_dump_period_sec = 60;
    return options;
}

bool DBManager::write_batch(const std::vector<ChangeSetRecord>& changes, 
                           const std::vector<IndexRecord>& indices) {
    if (!is_open_) return false;
    
    rocksdb::WriteBatch batch;
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    
    for (const auto& change : changes) {
        batch.Put(change.to_key(), change.value);
    }
    
    for (const auto& index : indices) {
        std::string serialized = serialize_block_list(index.block_history);
        batch.Merge(index.to_key(), serialized);
        
        // Debug logging
        utils::log_debug("Index merge: page {} addr_slot {} blocks {}", 
                        index.page_num, index.addr_slot.substr(0, 20), index.block_history.size());
    }
    
    rocksdb::Status status = db_->Write(write_options, &batch);
    if (!status.ok()) {
        utils::log_error("Write batch failed: {}", status.ToString());
    }
    return status.ok();
}

std::optional<Value> DBManager::get_historical_state(const std::string& addr_slot, 
                                                    BlockNum target_block_num) {
    if (!is_open_) return std::nullopt;
    
    PageNum target_page = block_to_page(target_block_num);
    IndexRecord index_query{target_page, addr_slot, {}};
    
    std::string index_data;
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), index_query.to_key(), &index_data);
    
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
    status = db_->Get(rocksdb::ReadOptions(), changeset_query.to_key(), &value);
    
    if (status.ok()) {
        utils::log_debug("Found value for block {} (target: {}) addr_slot {}", 
                        closest_block, target_block_num, addr_slot.substr(0, 20));
        return value;
    } else {
        utils::log_debug("Value not found for block {} addr_slot {}", closest_block, addr_slot.substr(0, 20));
        return std::nullopt;
    }
}

std::vector<BlockNum> DBManager::deserialize_block_list(const std::string& data) {
    std::vector<BlockNum> result;
    if (data.size() % sizeof(BlockNum) != 0) return result;
    
    size_t count = data.size() / sizeof(BlockNum);
    result.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        BlockNum block;
        std::memcpy(&block, data.data() + i * sizeof(BlockNum), sizeof(BlockNum));
        result.push_back(block);
    }
    
    return result;
}

std::string DBManager::serialize_block_list(const std::vector<BlockNum>& blocks) {
    std::string result;
    result.resize(blocks.size() * sizeof(BlockNum));
    
    for (size_t i = 0; i < blocks.size(); ++i) {
        std::memcpy(result.data() + i * sizeof(BlockNum), &blocks[i], sizeof(BlockNum));
    }
    
    return result;
}

bool IndexMergeOperator::FullMergeV2(const MergeOperationInput& merge_in,
                                    MergeOperationOutput* merge_out) const {
    std::vector<BlockNum> result;
    size_t total_merged_values = 0;
    
    if (merge_in.existing_value) {
        std::string existing_str(merge_in.existing_value->data(), merge_in.existing_value->size());
        result = merge_deserialize_block_list(existing_str);
        total_merged_values += result.size();
    }
    
    for (const auto& operand : merge_in.operand_list) {
        std::string operand_str(operand.data(), operand.size());
        std::vector<BlockNum> operand_blocks = merge_deserialize_block_list(operand_str);
        result.insert(result.end(), operand_blocks.begin(), operand_blocks.end());
        total_merged_values += operand_blocks.size();
    }
    
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    
    merge_out->new_value = merge_serialize_block_list(result);
    
    // Record merge metrics
    if (merge_callback_ && !merge_in.operand_list.empty()) {
        merge_callback_(total_merged_values, merge_out->new_value.size());
    }
    
    return true;
}

bool IndexMergeOperator::PartialMergeMulti(const rocksdb::Slice& key,
                                        const std::deque<rocksdb::Slice>& operand_list,
                                        std::string* new_value,
                                        rocksdb::Logger* logger) const {
    std::vector<BlockNum> result;
    size_t total_merged_values = 0;
    
    for (const auto& operand : operand_list) {
        std::string operand_str(operand.data(), operand.size());
        std::vector<BlockNum> operand_blocks = merge_deserialize_block_list(operand_str);
        result.insert(result.end(), operand_blocks.begin(), operand_blocks.end());
        total_merged_values += operand_blocks.size();
    }
    
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    
    *new_value = merge_serialize_block_list(result);
    
    // Record merge metrics
    if (merge_callback_ && !operand_list.empty()) {
        merge_callback_(total_merged_values, new_value->size());
    }
    
    return true;
}

std::vector<BlockNum> IndexMergeOperator::merge_deserialize_block_list(const std::string& data) {
    std::vector<BlockNum> result;
    if (data.size() % sizeof(BlockNum) != 0) return result;
    
    size_t count = data.size() / sizeof(BlockNum);
    result.reserve(count);
    
    for (size_t i = 0; i < count; ++i) {
        BlockNum block;
        std::memcpy(&block, data.data() + i * sizeof(BlockNum), sizeof(BlockNum));
        result.push_back(block);
    }
    
    return result;
}

std::string IndexMergeOperator::merge_serialize_block_list(const std::vector<BlockNum>& blocks) {
    std::string result;
    result.resize(blocks.size() * sizeof(BlockNum));
    
    for (size_t i = 0; i < blocks.size(); ++i) {
        std::memcpy(result.data() + i * sizeof(BlockNum), &blocks[i], sizeof(BlockNum));
    }
    
    return result;
}