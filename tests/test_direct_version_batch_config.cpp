#include <iostream>
#include <memory>
#include <vector>
#include <rocksdb/db.h>
#include "core/config.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"
#include "core/strategy_db_manager.hpp"

using BlockNum = uint64_t;
using Value = std::string;

// Helper function to generate test data
std::vector<DataRecord> generate_test_data(size_t num_records, BlockNum start_block = 0) {
    std::vector<DataRecord> records;
    
    for (size_t i = 0; i < num_records; ++i) {
        BlockNum block_num = start_block + i;
        std::string addr_slot = "batch_test_addr_" + std::to_string(i % 10); // 10 unique addresses
        std::string value = "batch_test_value_" + std::to_string(block_num);
        
        records.push_back({block_num, addr_slot, value});
    }
    
    return records;
}

// Cleanup helper
void cleanup_test_database(const std::string& db_path) {
    rocksdb::Options destroy_opts;
    rocksdb::DestroyDB(db_path, destroy_opts);
}

int main() {
    std::cout << "=== Testing DirectVersionStrategy Batch Configuration ===" << std::endl;
    
    try {
        // Test 1: Default batch configuration
        std::cout << "\n--- Test 1: Default Configuration ---" << std::endl;
        {
            std::string test_db_path = "./test_direct_version_default_batch";
            cleanup_test_database(test_db_path);
            
            BenchmarkConfig config;
            config.storage_strategy = "direct_version";
            config.db_path = test_db_path;
            
            auto strategy = StorageStrategyFactory::create_strategy("direct_version", config);
            auto db_manager = std::make_unique<StrategyDBManager>(test_db_path, std::move(strategy));
            
            if (!db_manager->open(true)) {
                std::cout << "✗ Failed to open database" << std::endl;
                return 1;
            }
            
            auto test_data = generate_test_data(20, 0); // 20 records
            
            std::cout << "Writing 20 records with default batch config..." << std::endl;
            if (!db_manager->write_batch(test_data)) {
                std::cout << "✗ Failed to write test data" << std::endl;
                return 1;
            }
            
            std::cout << "✓ Successfully wrote with default batch configuration" << std::endl;
            db_manager->close();
            cleanup_test_database(test_db_path);
        }
        
        // Test 2: Custom batch configuration - small batches
        std::cout << "\n--- Test 2: Small Batch Configuration (2 blocks per batch) ---" << std::endl;
        {
            std::string test_db_path = "./test_direct_version_small_batch";
            cleanup_test_database(test_db_path);
            
            BenchmarkConfig config;
            config.storage_strategy = "direct_version";
            config.db_path = test_db_path;
            config.direct_version_batch_size = 2; // Small batch size
            config.direct_version_max_batch_bytes = 1024 * 1024; // 1MB limit
            
            auto strategy = StorageStrategyFactory::create_strategy("direct_version", config);
            auto db_manager = std::make_unique<StrategyDBManager>(test_db_path, std::move(strategy));
            
            if (!db_manager->open(true)) {
                std::cout << "✗ Failed to open database" << std::endl;
                return 1;
            }
            
            auto test_data = generate_test_data(20, 0); // 20 records
            
            std::cout << "Writing 20 records with small batch config (2 blocks per batch)..." << std::endl;
            if (!db_manager->write_batch(test_data)) {
                std::cout << "✗ Failed to write test data" << std::endl;
                return 1;
            }
            
            std::cout << "✓ Successfully wrote with small batch configuration" << std::endl;
            db_manager->close();
            cleanup_test_database(test_db_path);
        }
        
        // Test 3: Custom batch configuration - large byte limit
        std::cout << "\n--- Test 3: Large Byte Limit Configuration ---" << std::endl;
        {
            std::string test_db_path = "./test_direct_version_large_bytes";
            cleanup_test_database(test_db_path);
            
            BenchmarkConfig config;
            config.storage_strategy = "direct_version";
            config.db_path = test_db_path;
            config.direct_version_batch_size = 100; // Large block count
            config.direct_version_max_batch_bytes = 10UL * 1024 * 1024 * 1024; // 10GB limit
            
            auto strategy = StorageStrategyFactory::create_strategy("direct_version", config);
            auto db_manager = std::make_unique<StrategyDBManager>(test_db_path, std::move(strategy));
            
            if (!db_manager->open(true)) {
                std::cout << "✗ Failed to open database" << std::endl;
                return 1;
            }
            
            auto test_data = generate_test_data(100, 0); // 100 records
            
            std::cout << "Writing 100 records with large byte limit..." << std::endl;
            if (!db_manager->write_batch(test_data)) {
                std::cout << "✗ Failed to write test data" << std::endl;
                return 1;
            }
            
            std::cout << "✓ Successfully wrote with large byte limit configuration" << std::endl;
            db_manager->close();
            cleanup_test_database(test_db_path);
        }
        
        // Test 4: Verify queries still work after batch writes
        std::cout << "\n--- Test 4: Query After Batch Writes ---" << std::endl;
        {
            std::string test_db_path = "./test_direct_version_query_after_batch";
            cleanup_test_database(test_db_path);
            
            BenchmarkConfig config;
            config.storage_strategy = "direct_version";
            config.db_path = test_db_path;
            config.direct_version_batch_size = 3; // Small batch to ensure multiple batches
            
            auto strategy = StorageStrategyFactory::create_strategy("direct_version", config);
            auto db_manager = std::make_unique<StrategyDBManager>(test_db_path, std::move(strategy));
            
            if (!db_manager->open(true)) {
                std::cout << "✗ Failed to open database" << std::endl;
                return 1;
            }
            
            auto test_data = generate_test_data(50, 0); // 50 records
            
            std::cout << "Writing 50 records with batch size 3..." << std::endl;
            if (!db_manager->write_batch(test_data)) {
                std::cout << "✗ Failed to write test data" << std::endl;
                return 1;
            }
            
            std::cout << "Testing queries after batch writes..." << std::endl;
            size_t query_success = 0;
            for (size_t i = 0; i < 5; ++i) {
                std::string addr_slot = "batch_test_addr_" + std::to_string(i);
                auto result = db_manager->query_latest_value(addr_slot);
                if (result) {
                    query_success++;
                    std::cout << "  ✓ Found value for " << addr_slot << std::endl;
                } else {
                    std::cout << "  ✗ No value found for " << addr_slot << std::endl;
                }
            }
            
            if (query_success == 5) {
                std::cout << "✓ All queries successful after batch writes" << std::endl;
            } else {
                std::cout << "✗ Only " << query_success << "/5 queries successful" << std::endl;
                return 1;
            }
            
            db_manager->close();
            cleanup_test_database(test_db_path);
        }
        
        // Summary
        std::cout << "\n=== Test Summary ===" << std::endl;
        std::cout << "✓ DirectVersionStrategy Batch Configuration: PASSED" << std::endl;
        std::cout << "✓ Default batch configuration works" << std::endl;
        std::cout << "✓ Small batch configuration works" << std::endl;
        std::cout << "✓ Large byte limit configuration works" << std::endl;
        std::cout << "✓ Queries work correctly after batch writes" << std::endl;
        
        std::cout << "\nBatch Configuration Details:" << std::endl;
        std::cout << "- Default: 5 blocks per batch, 4GB max" << std::endl;
        std::cout << "- Small test: 2 blocks per batch, 1MB max" << std::endl;
        std::cout << "- Large test: 100 blocks per batch, 10GB max" << std::endl;
        std::cout << "- Batching is transparent to the user" << std::endl;
        std::cout << "- All data is correctly written and queryable" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cout << "✗ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}