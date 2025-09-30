#include "core/db_manager.hpp"
#include "utils/logger.hpp"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    utils::log_info("Testing Bloom Filter functionality...");
    
    std::string db_path = "/tmp/test_bloom_simple";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    utils::log_info("Database opened with Bloom Filter enabled");
    
    // Check initial statistics
    utils::log_info("Initial statistics:");
    db_manager->debug_bloom_filter_stats();
    
    // Write some test data
    utils::log_info("Writing test data...");
    std::vector<ChangeSetRecord> changes;
    std::vector<IndexRecord> indices;
    
    for (int i = 0; i < 1000; ++i) {
        std::string key = "key" + std::to_string(i);
        std::string value = "value" + std::to_string(i);
        changes.push_back({static_cast<BlockNum>(i), key, value});
        
        PageNum page = block_to_page(static_cast<BlockNum>(i));
        indices.push_back({page, key, {static_cast<BlockNum>(i)}});
    }
    
    bool write_success = db_manager->write_batch(changes, indices);
    utils::log_info("Write success: {}", write_success);
    
    // Check statistics after write
    utils::log_info("Statistics after write:");
    db_manager->debug_bloom_filter_stats();
    
    // Perform some queries to generate bloom filter activity
    utils::log_info("Performing queries to generate bloom filter activity...");
    for (int i = 0; i < 500; ++i) {
        std::string key = "key" + std::to_string(i % 1000); // Some hits, some misses
        auto result = db_manager->get_historical_state(key, static_cast<BlockNum>(i % 1000));
        if (i % 100 == 0) {
            utils::log_info("Query {} result: {}", i, result.has_value());
        }
    }
    
    // Check final statistics
    utils::log_info("Final statistics after queries:");
    db_manager->debug_bloom_filter_stats();
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    utils::log_info("Bloom Filter test completed");
    return 0;
}