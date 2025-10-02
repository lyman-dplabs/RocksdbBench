#include "../src/strategies/dual_rocksdb_strategy.hpp"
#include "../src/utils/logger.hpp"
#include <iostream>
#include <filesystem>
#include <rocksdb/db.h>

int main() {
    std::cout << "=== Test DualRocksDBStrategy Historical Version Query ===" << std::endl;
    
    try {
        // 创建测试数据库
        std::string test_db_path = "/tmp/test_dual_strategy_basic";
        std::filesystem::create_directories(test_db_path);
        
        // 创建DualRocksDBStrategy
        DualRocksDBStrategy::Config config;
        config.range_size = 5000;
        config.max_cache_memory = 1024 * 1024; // 1MB
        config.hot_cache_ratio = 0.1;
        config.medium_cache_ratio = 0.2;
        
        auto strategy = std::make_unique<DualRocksDBStrategy>(config);
        
        // 打开数据库
        rocksdb::DB* db;
        rocksdb::Options options;
        options.create_if_missing = true;
        
        rocksdb::Status status = rocksdb::DB::Open(options, test_db_path, &db);
        if (!status.ok()) {
            std::cerr << "Failed to open database: " << status.ToString() << std::endl;
            return 1;
        }
        
        std::cout << "Database opened successfully" << std::endl;
        
        // 初始化strategy
        if (!strategy->initialize(db)) {
            std::cerr << "Failed to initialize strategy" << std::endl;
            return 1;
        }
        
        std::cout << "Strategy initialized successfully" << std::endl;
        
        // 写入测试数据
        std::string test_key = "0x1234567890abcdef1234567890abcdef12345678";
        std::vector<DataRecord> test_records = {
            {0, test_key, "value_at_block_0"},
            {5, test_key, "value_at_block_5"},
            {8, test_key, "value_at_block_8"}
        };
        
        std::cout << "Writing test records..." << std::endl;
        for (const auto& record : test_records) {
            std::cout << "  Block " << record.block_num << ": " << record.addr_slot << " -> " << record.value << std::endl;
        }
        
        bool write_success = strategy->write_batch(db, test_records);
        std::cout << "Write result: " << (write_success ? "SUCCESS" : "FAILED") << std::endl;
        
        if (!write_success) {
            delete db;
            std::filesystem::remove_all(test_db_path);
            return 1;
        }
        
        // 测试query_latest_value
        std::cout << "\nTesting query_latest_value..." << std::endl;
        auto latest_result = strategy->query_latest_value(db, test_key);
        if (latest_result.has_value()) {
            std::cout << "Latest value: " << *latest_result << std::endl;
        } else {
            std::cout << "No latest value found!" << std::endl;
        }
        
        // 测试query_historical_version
        std::cout << "\nTesting query_historical_version..." << std::endl;
        
        std::vector<BlockNum> test_versions = {10, 8, 6, 5, 3, 1, 0};
        for (BlockNum version : test_versions) {
            std::cout << "Query version " << version << ": ";
            auto result = strategy->query_historical_version(db, test_key, version);
            if (result.has_value()) {
                std::cout << *result << std::endl;
            } else {
                std::cout << "NOT FOUND" << std::endl;
            }
        }
        
        // 清理
        strategy->cleanup(db);
        delete db;
        std::filesystem::remove_all(test_db_path);
        
        std::cout << "\nTest completed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}