#include <iostream>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <cstdint>

int main() {
    rocksdb::DB* range_db = nullptr;
    rocksdb::DB* data_db = nullptr;
    rocksdb::Options options;
    options.create_if_missing = false;
    
    std::string range_db_path = "/home/jingyue/work/dplabs-intl/rocksdb_bench/rocksdb_data_initial_load_test_range_index";
    std::string data_db_path = "/home/jingyue/work/dplabs-intl/rocksdb_bench/rocksdb_data_initial_load_test_data_storage";
    
    // Open both databases
    rocksdb::Status range_status = rocksdb::DB::Open(options, range_db_path, &range_db);
    rocksdb::Status data_status = rocksdb::DB::Open(options, data_db_path, &data_db);
    
    if (!range_status.ok() || !data_status.ok()) {
        std::cerr << "Failed to open databases: " << range_status.ToString() << ", " << data_status.ToString() << std::endl;
        return 1;
    }
    
    std::cout << "=== Analyzing Block Distribution in Data ===" << std::endl;
    
    // Analyze block distribution in data
    std::unordered_map<uint64_t, size_t> block_counts;
    std::unordered_map<std::string, std::vector<uint64_t>> address_blocks;
    
    rocksdb::Iterator* data_it = data_db->NewIterator(rocksdb::ReadOptions());
    size_t total_data_keys = 0;
    
    for (data_it->SeekToFirst(); data_it->Valid(); data_it->Next()) {
        std::string key = data_it->key().ToString();
        total_data_keys++;
        
        // Parse key format: R{range_num}|{addr_slot}|{block_number}
        if (key.rfind("R", 0) == 0) {
            size_t first_pipe = key.find('|');
            size_t last_pipe = key.find_last_of('|');
            
            if (first_pipe != std::string::npos && last_pipe != std::string::npos && first_pipe < last_pipe) {
                std::string addr_slot = key.substr(first_pipe + 1, last_pipe - first_pipe - 1);
                std::string block_str = key.substr(last_pipe + 1);
                
                try {
                    uint64_t block_num = std::stoull(block_str);
                    block_counts[block_num]++;
                    address_blocks[addr_slot].push_back(block_num);
                } catch (const std::exception& e) {
                    continue;
                }
            }
        }
        
        if (total_data_keys % 1000000 == 0) {
            std::cout << "Processed " << total_data_keys << " data keys..." << std::endl;
        }
    }
    
    delete data_it;
    
    std::cout << "\n=== Block Distribution Results ===" << std::endl;
    std::cout << "Total data keys: " << total_data_keys << std::endl;
    std::cout << "Unique blocks: " << block_counts.size() << std::endl;
    
    if (!block_counts.empty()) {
        uint64_t min_block = UINT64_MAX;
        uint64_t max_block = 0;
        for (const auto& [block, count] : block_counts) {
            if (block < min_block) min_block = block;
            if (block > max_block) max_block = block;
        }
        std::cout << "Block range: " << min_block << " to " << max_block << std::endl;
        
        // Count blocks in the query range [100, 151]
        size_t query_range_blocks = 0;
        for (uint64_t block = 100; block <= 151; ++block) {
            if (block_counts.find(block) != block_counts.end()) {
                query_range_blocks++;
            }
        }
        std::cout << "Blocks in query range [100, 151]: " << query_range_blocks << std::endl;
        
        // Show some sample block counts
        std::cout << "\nSample block counts:" << std::endl;
        for (uint64_t block = 0; block < 20 && block <= max_block; ++block) {
            auto it = block_counts.find(block);
            if (it != block_counts.end()) {
                std::cout << "  Block " << block << ": " << it->second << " keys" << std::endl;
            }
        }
        
        std::cout << "\nBlocks in query range [100, 151]:" << std::endl;
        for (uint64_t block = 100; block <= 151; ++block) {
            auto it = block_counts.find(block);
            if (it != block_counts.end()) {
                std::cout << "  Block " << block << ": " << it->second << " keys" << std::endl;
            }
        }
    }
    
    // Analyze address coverage in query range
    std::cout << "\n=== Address Coverage Analysis ===" << std::endl;
    size_t total_addresses = address_blocks.size();
    size_t addresses_in_query_range = 0;
    
    for (const auto& [addr, blocks] : address_blocks) {
        bool has_data_in_query_range = false;
        for (uint64_t block : blocks) {
            if (block >= 100 && block <= 151) {
                has_data_in_query_range = true;
                break;
            }
        }
        if (has_data_in_query_range) {
            addresses_in_query_range++;
        }
    }
    
    std::cout << "Total addresses: " << total_addresses << std::endl;
    std::cout << "Addresses with data in query range [100, 151]: " << addresses_in_query_range << std::endl;
    std::cout << "Coverage percentage: " << (addresses_in_query_range * 100.0 / total_addresses) << "%" << std::endl;
    
    // This should explain the ~25% success rate!
    
    delete range_db;
    delete data_db;
    
    return 0;
}