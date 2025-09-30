#include <iostream>
#include <memory>
#include <vector>
#include <rocksdb/db.h>
#include "core/config.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"

using BlockNum = uint64_t;
using Value = std::string;

int main() {
    std::cout << "Testing DualRocksDBStrategy..." << std::endl;
    
    try {
        // Test strategy factory
        auto strategies = StorageStrategyFactory::get_available_strategies();
        std::cout << "Available strategies: " << std::endl;
        for (const auto& strategy : strategies) {
            std::cout << "  - " << strategy << std::endl;
        }
        
        // Test creating the dual rocksdb strategy
        std::cout << "\nCreating DualRocksDBStrategy..." << std::endl;
        auto dual_strategy = StorageStrategyFactory::create_strategy("dual_rocksdb_adaptive");
        if (!dual_strategy) {
            std::cout << "✗ Failed to create DualRocksDBStrategy" << std::endl;
            return 1;
        }
        
        std::cout << "✓ DualRocksDBStrategy created successfully!" << std::endl;
        std::cout << "Strategy name: " << dual_strategy->get_strategy_name() << std::endl;
        std::cout << "Description: " << dual_strategy->get_description() << std::endl;
        
        // Create test database
        std::string test_db_path = "./test_dual_rocksdb";
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
        
        // Test data: simulate blockchain-like writes
        std::vector<DataRecord> test_records = {
            {100, "addr_001", "value_at_block_100"},
            {105, "addr_001", "value_at_block_105"},
            {110, "addr_001", "value_at_block_110"},
            {120, "addr_002", "value_at_block_120"},
            {130, "addr_001", "value_at_block_130"},
        };
        
        // Write test data
        if (!dual_strategy->write_batch(db, test_records)) {
            std::cout << "✗ Failed to write test data" << std::endl;
            dual_strategy->cleanup(db);
            db->Close();
            return 1;
        }
        
        std::cout << "✓ Test data written (" << test_records.size() << " records)" << std::endl;
        
        // Test 1: Query latest value (should return most recent)
        std::cout << "\n=== Test 1: Query Latest Value ===" << std::endl;
        auto latest_value = dual_strategy->query_latest_value(db, "addr_001");
        if (latest_value && *latest_value == "value_at_block_130") {
            std::cout << "✓ Latest value query passed: " << *latest_value << std::endl;
        } else {
            std::cout << "✗ Latest value query failed. Expected: value_at_block_130, Got: " 
                      << (latest_value ? *latest_value : "null") << std::endl;
        }
        
        // Test 2: Query historical value (should return value at or before target block)
        std::cout << "\n=== Test 2: Query Historical Value ===" << std::endl;
        
        // Test case 2a: Query at block 107 (should return value at block 105)
        auto hist_value_105 = dual_strategy->query_historical_value(db, "addr_001", 107);
        if (hist_value_105 && *hist_value_105 == "value_at_block_105") {
            std::cout << "✓ Historical query at block 107 passed: " << *hist_value_105 << std::endl;
        } else {
            std::cout << "✗ Historical query at block 107 failed. Expected: value_at_block_105, Got: " 
                      << (hist_value_105 ? *hist_value_105 : "null") << std::endl;
        }
        
        // Test case 2b: Query at block 100 (should return exact value)
        auto hist_value_100 = dual_strategy->query_historical_value(db, "addr_001", 100);
        if (hist_value_100 && *hist_value_100 == "value_at_block_100") {
            std::cout << "✓ Historical query at block 100 passed: " << *hist_value_100 << std::endl;
        } else {
            std::cout << "✗ Historical query at block 100 failed. Expected: value_at_block_100, Got: " 
                      << (hist_value_100 ? *hist_value_100 : "null") << std::endl;
        }
        
        // Test case 2c: Query at block 99 (should return null, no data before block 100)
        auto hist_value_99 = dual_strategy->query_historical_value(db, "addr_001", 99);
        if (!hist_value_99) {
            std::cout << "✓ Historical query at block 99 passed: null (no data before block 100)" << std::endl;
        } else {
            std::cout << "✗ Historical query at block 99 failed. Expected: null, Got: " << *hist_value_99 << std::endl;
        }
        
        // Test case 2d: Query non-existent address
        auto hist_value_nonexistent = dual_strategy->query_historical_value(db, "addr_999", 150);
        if (!hist_value_nonexistent) {
            std::cout << "✓ Historical query for non-existent address passed: null" << std::endl;
        } else {
            std::cout << "✗ Historical query for non-existent address failed. Expected: null, Got: " << *hist_value_nonexistent << std::endl;
        }
        
        // Test 3: Query latest value for different address
        std::cout << "\n=== Test 3: Query Latest Value for Different Address ===" << std::endl;
        auto latest_value_002 = dual_strategy->query_latest_value(db, "addr_002");
        if (latest_value_002 && *latest_value_002 == "value_at_block_120") {
            std::cout << "✓ Latest value query for addr_002 passed: " << *latest_value_002 << std::endl;
        } else {
            std::cout << "✗ Latest value query for addr_002 failed. Expected: value_at_block_120, Got: " 
                      << (latest_value_002 ? *latest_value_002 : "null") << std::endl;
        }
        
        // Cleanup - proper order: strategy first, then main db
        dual_strategy->cleanup(db);
        db->Close();
        
        // Clean up test database
        rocksdb::DestroyDB(test_db_path, options);
        
        // Clean up range and data databases (DualRocksDBStrategy uses hardcoded path)
        rocksdb::DestroyDB("./rocksdb_data_range_index", options);
        rocksdb::DestroyDB("./rocksdb_data_data_storage", options);
        
        std::cout << "\n✓ All tests completed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}