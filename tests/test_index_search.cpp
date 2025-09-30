#include "core/db_manager.hpp"
#include "utils/logger.hpp"
#include <iostream>
#include <cassert>

int main() {
    utils::log_info("Testing Index table search logic...");
    
    std::string db_path = "/tmp/test_index_search";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    utils::log_info("Database opened successfully");
    
    // Write some test data to create Index entries
    std::vector<ChangeSetRecord> changes;
    std::vector<IndexRecord> indices;
    
    // Write data across multiple blocks/pages
    for (int block = 0; block < 50; ++block) {
        for (int i = 0; i < 100; ++i) {
            std::string key = "key" + std::to_string(i);
            std::string value = "value_block_" + std::to_string(block) + "_" + std::to_string(i);
            changes.push_back({static_cast<BlockNum>(block), key, value});
            
            PageNum page = block_to_page(static_cast<BlockNum>(block));
            indices.push_back({page, key, {static_cast<BlockNum>(block)}});
        }
    }
    
    bool write_success = db_manager->write_batch(changes, indices);
    utils::log_info("Write success: {}", write_success);
    
    // Test finding latest block for a few keys
    utils::log_info("Testing Index table search for latest blocks...");
    
    for (int test_key = 0; test_key < 5; ++test_key) {
        std::string key = "key" + std::to_string(test_key);
        
        // Use the method to find the latest block
        auto latest_block_opt = db_manager->find_latest_block_for_key(key, 50);
        
        if (latest_block_opt) {
            BlockNum latest_block = *latest_block_opt;
            utils::log_info("Key {}: latest block = {}", key, latest_block);
            
            // Verify we can actually get the value
            auto result = db_manager->get_historical_state(key, latest_block);
            utils::log_info("  -> Query result: {}", result.has_value());
            
            // Also test querying a different block (should work too)
            if (latest_block > 0) {
                auto earlier_result = db_manager->get_historical_state(key, latest_block - 1);
                utils::log_info("  -> Query block {} result: {}", latest_block - 1, earlier_result.has_value());
            }
        } else {
            utils::log_error("Failed to find latest block for key {}", key);
        }
    }
    
    // Test performance with more queries
    utils::log_info("Testing performance with 1000 queries...");
    auto start = std::chrono::high_resolution_clock::now();
    
    int found_count = 0;
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key" + std::to_string(i % 100); // Cycle through keys 0-99
        auto result = db_manager->find_latest_block_for_key(key, 50);
        if (result) found_count++;
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    utils::log_info("Performance test: {} found out of {} queries in {} ms", 
                   found_count, 1000, duration.count());
    utils::log_info("Average time per query: {:.2f} ms", 
                   duration.count() / 1000.0);
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    utils::log_info("Index table search test completed");
    return 0;
}