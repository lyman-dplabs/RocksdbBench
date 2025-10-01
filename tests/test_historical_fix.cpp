#include <iostream>
#include <memory>
#include <vector>
#include <rocksdb/db.h>
#include "core/config.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"
#include "strategies/dual_rocksdb_strategy.hpp"

using BlockNum = uint64_t;
using Value = std::string;

int main() {
    std::cout << "Testing DualRocksDBStrategy Historical Query Fix..." << std::endl;
    
    try {
        // Test strategy factory
        auto strategies = StorageStrategyFactory::get_available_strategies();
        std::cout << "Available strategies: " << std::endl;
        for (const auto& strategy : strategies) {
            std::cout << "  - " << strategy << std::endl;
        }
        
        // Test creating the dual rocksdb strategy
        std::cout << "\nCreating DualRocksDBStrategy..." << std::endl;
        BenchmarkConfig test_config;
        auto dual_strategy = StorageStrategyFactory::create_strategy("dual_rocksdb_adaptive", test_config);
        if (!dual_strategy) {
            std::cout << "✗ Failed to create DualRocksDBStrategy" << std::endl;
            return 1;
        }
        
        std::cout << "✓ DualRocksDBStrategy created successfully!" << std::endl;
        
        // Create test database
        std::string test_db_path = "./test_historical_fix";
        rocksdb::Options options;
        options.create_if_missing = true;
        options.error_if_exists = true;
        
        rocksdb::DB* db;
        rocksdb::Status status = rocksdb::DB::Open(options, test_db_path, &db);
        if (!status.ok()) {
            std::cout << "✗ Failed to create test database: " << status.ToString() << std::endl;
            return 1;
        }
        
        std::cout << "✓ Test database created" << std::endl;
        
        // Initialize strategy
        if (!dual_strategy->initialize(db)) {
            std::cout << "✗ Failed to initialize strategy" << std::endl;
            db->Close();
            return 1;
        }
        
        std::cout << "✓ Strategy initialized" << std::endl;
        
        // Test data: simulate blockchain-like writes with different ranges
        // Range size is 10000, so:
        // Block 100 -> Range 0
        // Block 15000 -> Range 1  
        // Block 25000 -> Range 2
        std::vector<DataRecord> test_records = {
            {100, "addr_001", "value_at_block_100"},      // Range 0
            {500, "addr_001", "value_at_block_500"},      // Range 0
            {15000, "addr_001", "value_at_block_15000"},  // Range 1
            {16000, "addr_001", "value_at_block_16000"},  // Range 1
            {25000, "addr_001", "value_at_block_25000"},  // Range 2
            {26000, "addr_001", "value_at_block_26000"},  // Range 2
        };
        
        // Write test data
        if (!dual_strategy->write_batch(db, test_records)) {
            std::cout << "✗ Failed to write test data" << std::endl;
            dual_strategy->cleanup(db);
            db->Close();
            return 1;
        }
        
        std::cout << "✓ Test data written (" << test_records.size() << " records)" << std::endl;
        
        // Test cases for historical queries
        struct TestCase {
            BlockNum query_block;
            std::string expected_value;
            std::string description;
        };
        
        std::vector<TestCase> test_cases = {
            // Query within Range 0 - should find latest block <= target in Range 0
            {200, "value_at_block_100", "Query within Range 0 (block 200)"},
            
            // Query at exact block
            {100, "value_at_block_100", "Query at exact block 100"},
            {500, "value_at_block_500", "Query at exact block 500"},
            
            // Query between Range 0 and Range 1 - should find latest in Range 0
            {1000, "value_at_block_500", "Query between Range 0 and 1 (block 1000)"},
            
            // Query within Range 1 - should find latest block <= target in Range 1
            {15500, "value_at_block_15000", "Query within Range 1 (block 15500)"},
            
            // Query between Range 1 and Range 2 - should find latest in Range 1
            {20000, "value_at_block_16000", "Query between Range 1 and 2 (block 20000)"},
            
            // Query within Range 2 - should find latest block <= target in Range 2
            {25500, "value_at_block_25000", "Query within Range 2 (block 25500)"},
            
            // Query after all data - should find latest overall
            {30000, "value_at_block_26000", "Query after all data (block 30000)"},
            
            // Query before any data - should return null
            {50, "", "Query before any data (block 50)"},
        };
        
        std::cout << "\n=== Historical Query Tests ===" << std::endl;
        int passed = 0;
        int total = test_cases.size();
        
        for (const auto& test_case : test_cases) {
            std::cout << "\nTest: " << test_case.description << std::endl;
            
            auto result = dual_strategy->query_historical_value(db, "addr_001", test_case.query_block);
            
            if (test_case.expected_value.empty()) {
                // Expect null result
                if (!result) {
                    std::cout << "✓ Passed: Query returned null as expected" << std::endl;
                    passed++;
                } else {
                    std::cout << "✗ Failed: Expected null, got: " << *result << std::endl;
                }
            } else {
                // Expect specific value
                if (result && *result == test_case.expected_value) {
                    std::cout << "✓ Passed: Query returned " << *result << std::endl;
                    passed++;
                } else {
                    std::cout << "✗ Failed: Expected " << test_case.expected_value 
                              << ", got: " << (result ? *result : "null") << std::endl;
                }
            }
        }
        
        // Test latest value query
        std::cout << "\n=== Debug: Scan all keys in databases ===" << std::endl;
        
        // Access the internal databases to debug
        auto* dual_ptr = dynamic_cast<DualRocksDBStrategy*>(dual_strategy.get());
        if (dual_ptr) {
            // This is just for debugging - we'll access the strategy's internal DBs
            std::cout << "Strategy has databases" << std::endl;
        }
        
        std::cout << "\n=== Latest Value Query Test ===" << std::endl;
        auto latest_value = dual_strategy->query_latest_value(db, "addr_001");
        if (latest_value && *latest_value == "value_at_block_26000") {
            std::cout << "✓ Latest value query passed: " << *latest_value << std::endl;
            passed++;
        } else {
            std::cout << "✗ Latest value query failed. Expected: value_at_block_26000, Got: " 
                      << (latest_value ? *latest_value : "null") << std::endl;
        }
        total++;
        
        // Test non-existent address
        std::cout << "\n=== Non-existent Address Test ===" << std::endl;
        auto nonexistent_result = dual_strategy->query_historical_value(db, "addr_999", 15000);
        if (!nonexistent_result) {
            std::cout << "✓ Non-existent address query returned null as expected" << std::endl;
            passed++;
        } else {
            std::cout << "✗ Non-existent address query failed: " << *nonexistent_result << std::endl;
        }
        total++;
        
        // Print SST compaction stats
        std::cout << "\n=== SST Compaction Stats ===" << std::endl;
        auto* stats_ptr = dynamic_cast<DualRocksDBStrategy*>(dual_strategy.get());
        if (stats_ptr) {
            std::cout << "Compaction count: " << stats_ptr->get_compaction_count() << std::endl;
            std::cout << "Bytes written: " << stats_ptr->get_compaction_bytes_written() << std::endl;
            std::cout << "Bytes read: " << stats_ptr->get_compaction_bytes_read() << std::endl;
            std::cout << "Compaction efficiency: " << stats_ptr->get_compaction_efficiency() << std::endl;
        }
        
        // Summary
        std::cout << "\n=== Test Summary ===" << std::endl;
        std::cout << "Passed: " << passed << "/" << total << " (" 
                  << (100.0 * passed / total) << "%)" << std::endl;
        
        // Cleanup
        dual_strategy->cleanup(db);
        db->Close();
        
        // Clean up test database
        rocksdb::DestroyDB(test_db_path, options);
        rocksdb::DestroyDB(test_db_path + "_range_index", options);
        rocksdb::DestroyDB(test_db_path + "_data_storage", options);
        
        if (passed == total) {
            std::cout << "\n✓ All tests passed successfully!" << std::endl;
            return 0;
        } else {
            std::cout << "\n✗ Some tests failed!" << std::endl;
            return 1;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}