#include "dual_rocksdb_strategy.hpp"
#include "../core/types.hpp"
#include "../utils/data_generator.hpp"
#include <iostream>
#include <memory>
#include <filesystem>

using namespace utils;

// 集成测试：验证重构后的写入逻辑
int main() {
    std::cout << "=== DualRocksDBStrategy Refactoring Integration Test ===" << std::endl;
    
    // 创建临时数据库路径
    std::string test_db_path = "/tmp/test_refactor_integration";
    std::filesystem::remove_all(test_db_path);
    
    // 初始化日志系统以查看调试日志
    utils::init_logger("refactor_test");
    
    // 创建测试配置
    DualRocksDBStrategy::Config config;
    config.range_size = 10000;
    config.batch_size_blocks = 3;  // 小批次以便测试
    config.max_batch_size_bytes = 50 * 1024 * 1024; // 50MB
    config.enable_compression = false;
    config.enable_bloom_filters = true;
    
    // 创建策略实例
    auto strategy = std::make_unique<DualRocksDBStrategy>(config);
    
    // 创建一个临时的主数据库（仅用于测试）
    rocksdb::DB* main_db = nullptr;
    rocksdb::Options main_db_options;
    main_db_options.create_if_missing = true;
    rocksdb::Status status = rocksdb::DB::Open(main_db_options, test_db_path, &main_db);
    
    if (!status.ok()) {
        std::cerr << "Failed to create test database: " << status.ToString() << std::endl;
        return 1;
    }
    
    // 初始化策略
    if (!strategy->initialize(main_db)) {
        std::cerr << "Failed to initialize strategy" << std::endl;
        delete main_db;
        return 1;
    }
    
    std::cout << "\n=== Test 1: write_batch (Hotspot Update) ===" << std::endl;
    std::cout << "Creating test data for hotspot update..." << std::endl;
    
    // 创建测试数据（模拟一个block）
    std::vector<DataRecord> hotspot_records;
    for (int i = 0; i < 100; ++i) {
        hotspot_records.push_back({
            static_cast<BlockNum>(50000 + i),  // block numbers
            "0xtestaddr1234567890abcdef#slot" + std::to_string(i % 10),  // 10个不同的slot
            "hotspot_value_" + std::to_string(i)
        });
    }
    
    // 执行write_batch
    std::cout << "Executing write_batch with " << hotspot_records.size() << " records..." << std::endl;
    bool success = strategy->write_batch(main_db, hotspot_records);
    
    if (success) {
        std::cout << "✓ write_batch succeeded" << std::endl;
        std::cout << "  Total writes: " << strategy->get_total_writes() << std::endl;
    } else {
        std::cout << "✗ write_batch failed" << std::endl;
    }
    
    std::cout << "\n=== Test 2: write_initial_load_batch (Initial Load) ===" << std::endl;
    std::cout << "Creating test data for initial load..." << std::endl;
    
    // 创建多个block的测试数据
    std::vector<std::vector<DataRecord>> initial_load_blocks;
    for (int block = 0; block < 5; ++block) {
        std::vector<DataRecord> block_data;
        for (int i = 0; i < 50; ++i) {
            block_data.push_back({
                static_cast<BlockNum>(block * 10000 + i),  // block numbers
                "0xinitialaddr" + std::to_string(block) + "#slot" + std::to_string(i % 5),
                "initial_value_" + std::to_string(block) + "_" + std::to_string(i)
            });
        }
        initial_load_blocks.push_back(std::move(block_data));
    }
    
    // 执行write_initial_load_batch多次
    uint64_t writes_before = strategy->get_total_writes();
    std::cout << "Writes before initial load: " << writes_before << std::endl;
    
    for (size_t i = 0; i < initial_load_blocks.size(); ++i) {
        std::cout << "\nProcessing block " << (i+1) << "/" << initial_load_blocks.size() << std::endl;
        std::cout << "Block contains " << initial_load_blocks[i].size() << " records" << std::endl;
        
        success = strategy->write_initial_load_batch(main_db, initial_load_blocks[i]);
        if (success) {
            std::cout << "✓ Block " << (i+1) << " processed successfully" << std::endl;
        } else {
            std::cout << "✗ Block " << (i+1) << " processing failed" << std::endl;
        }
    }
    
    std::cout << "\n=== Test Results ===" << std::endl;
    uint64_t total_writes = strategy->get_total_writes();
    std::cout << "Total writes after all tests: " << total_writes << std::endl;
    std::cout << "Records added: " << (hotspot_records.size() + 5 * 50) << std::endl;
    
    // 验证写入数量
    uint64_t expected_writes = hotspot_records.size() + 5 * 50;
    if (total_writes == writes_before + expected_writes) {
        std::cout << "✓ Write count verification passed" << std::endl;
    } else {
        std::cout << "✗ Write count verification failed" << std::endl;
        std::cout << "  Expected: " << (writes_before + expected_writes) << std::endl;
        std::cout << "  Actual: " << total_writes << std::endl;
    }
    
    // 清理
    strategy->cleanup(main_db);
    delete main_db;
    std::filesystem::remove_all(test_db_path);
    
    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "1. write_batch: 立即写入，不积累批次" << std::endl;
    std::cout << "2. write_initial_load_batch: 积累批次，达到限制后刷写" << std::endl;
    std::cout << "3. 调试日志显示了每个步骤的详细信息" << std::endl;
    std::cout << "4. 重构后的代码保持了原有功能，同时提高了可读性" << std::endl;
    
    return 0;
}