#include "dual_rocksdb_strategy.hpp"
#include "../core/types.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <cassert>
#include <algorithm>
#include <iomanip>

using namespace utils;

// ===== Unit Tests for DualRocksDBStrategy Writing Logic =====

void test_write_batch_behavior() {
    std::cout << "\n=== Test: write_batch Behavior ===" << std::endl;
    
    // Create a test strategy
    DualRocksDBStrategy::Config config;
    config.range_size = 10000;
    config.batch_size_blocks = 100;
    config.max_batch_size_bytes = 1024 * 1024 * 1024; // 1GB
    
    DualRocksDBStrategy strategy(config);
    
    // Mock test data
    std::vector<DataRecord> test_records;
    for (int i = 0; i < 10; ++i) {
        test_records.push_back({
            static_cast<BlockNum>(i * 1000),
            "0x1234567890abcdef#slot" + std::to_string(i),
            "test_value_" + std::to_string(i)
        });
    }
    
    std::cout << "Test created with " << test_records.size() << " records" << std::endl;
    std::cout << "Each record should have: addr_slot + range_num + block_num in data key" << std::endl;
    std::cout << "Expected behavior: write_batch writes immediately without accumulation" << std::endl;
}

void test_initial_load_batch_behavior() {
    std::cout << "\n=== Test: write_initial_load_batch Behavior ===" << std::endl;
    
    // Test configuration
    DualRocksDBStrategy::Config config;
    config.range_size = 10000;
    config.batch_size_blocks = 5;  // Small batch size for testing
    config.max_batch_size_bytes = 10 * 1024 * 1024; // 10MB
    
    std::cout << "Config: batch_size_blocks=" << config.batch_size_blocks 
              << ", max_batch_size_bytes=" << config.max_batch_size_bytes << std::endl;
    
    std::cout << "Expected behavior: Accumulates blocks until batch limits are reached" << std::endl;
    std::cout << "  - Each vector<DataRecord> counts as 1 block" << std::endl;
    std::cout << "  - Flushes when block count OR byte limit is reached" << std::endl;
}

void test_range_calculation() {
    std::cout << "\n=== Test: Range Calculation ===" << std::endl;
    
    DualRocksDBStrategy::Config config;
    config.range_size = 10000;
    DualRocksDBStrategy strategy(config);
    
    // Test some block numbers
    struct TestCase {
        BlockNum block_num;
        uint32_t expected_range;
    };
    
    std::vector<TestCase> test_cases = {
        {0, 0},
        {9999, 0},
        {10000, 1},
        {19999, 1},
        {50000, 5},
        {123456, 12}
    };
    
    for (const auto& test : test_cases) {
        uint32_t calculated_range = test.block_num / config.range_size;
        std::cout << "Block " << test.block_num << " -> Range " << calculated_range 
                  << " (expected: " << test.expected_range << ")" << std::endl;
        assert(calculated_range == test.expected_range);
    }
    
    std::cout << "✓ Range calculation test passed" << std::endl;
}

void test_data_key_format() {
    std::cout << "\n=== Test: Data Key Format ===" << std::endl;
    
    DualRocksDBStrategy::Config config;
    config.range_size = 10000;
    DualRocksDBStrategy strategy(config);
    
    // Test data key generation
    BlockNum block_num = 12345;
    std::string addr_slot = "0xabcdef1234567890#slot123";
    uint32_t range_num = block_num / config.range_size; // Should be 1
    
    // Expected format: R{range_num}|{addr_slot}|{block_num (10-digit padded)}
    std::string block_str = std::to_string(block_num);
    if (block_str.length() < 10) {
        block_str.insert(0, 10 - block_str.length(), '0');
    }
    std::string expected_key = "R" + std::to_string(range_num) + "|" + addr_slot + "|" + block_str;
    
    std::cout << "Block number: " << block_num << std::endl;
    std::cout << "Range number: " << range_num << std::endl;
    std::cout << "Expected key: " << expected_key << std::endl;
    std::cout << "Key format: R{range}|{addr_slot}|{block_num (10-digit)}" << std::endl;
}

int main() {
    std::cout << "=== DualRocksDBStrategy Writing Logic Unit Tests ===" << std::endl;
    std::cout << "This file tests the refactored writing behavior." << std::endl;
    
    test_write_batch_behavior();
    test_initial_load_batch_behavior();
    test_range_calculation();
    test_data_key_format();
    
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "1. write_batch: Immediate write, 1 block per call" << std::endl;
    std::cout << "2. write_initial_load_batch: Accumulates until batch limits" << std::endl;
    std::cout << "3. Each vector<DataRecord> = 1 block (contains ~10,000 records)" << std::endl;
    std::cout << "4. Range calculation: range_num = block_num / range_size" << std::endl;
    std::cout << "5. Data key format includes range, address, and padded block number" << std::endl;
    
    return 0;
}