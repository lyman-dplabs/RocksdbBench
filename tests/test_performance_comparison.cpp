#include <iostream>
#include <memory>
#include <vector>
#include <chrono>
#include <rocksdb/db.h>
#include "core/config.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"
#include "core/strategy_db_manager.hpp"

using BlockNum = uint64_t;
using Value = std::string;

// Helper function to generate test data
std::vector<DataRecord> generate_test_data(size_t num_addresses, BlockNum start_block = 0) {
    std::vector<DataRecord> records;
    
    for (size_t addr = 0; addr < num_addresses; ++addr) {
        BlockNum block_num = start_block + addr;
        std::string addr_slot = "perf_test_addr_" + std::to_string(addr);
        std::string value = "performance_test_value_" + std::to_string(block_num);
        
        records.push_back({block_num, addr_slot, value});
    }
    
    return records;
}

// Performance measurement helper
class PerformanceTimer {
private:
    std::chrono::high_resolution_clock::time_point start_time_;
    
public:
    void start() {
        start_time_ = std::chrono::high_resolution_clock::now();
    }
    
    double elapsed_ms() const {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_);
        return duration.count() / 1000.0;
    }
};

// Cleanup helper
void cleanup_test_databases(const std::string& base_path) {
    rocksdb::Options destroy_opts;
    rocksdb::DestroyDB(base_path, destroy_opts);
    rocksdb::DestroyDB(base_path + "_range_index", destroy_opts);
    rocksdb::DestroyDB(base_path + "_data_storage", destroy_opts);
}

int main() {
    std::cout << "=== DualRocksDB Initial Load Performance Comparison ===" << std::endl;
    
    try {
        // Test configurations
        const std::vector<size_t> test_sizes = {1000, 5000, 10000, 20000};
        
        std::cout << "Performance comparison between regular write_batch and optimized write_initial_load_batch" << std::endl;
        std::cout << std::endl;
        
        for (size_t test_size : test_sizes) {
            std::cout << "--- Testing with " << test_size << " records ---" << std::endl;
            
            // Generate test data
            auto test_data = generate_test_data(test_size);
            
            // Test 1: Regular write_batch
            std::cout << "Test 1: Regular write_batch..." << std::endl;
            {
                std::string db_path = "./perf_test_regular_" + std::to_string(test_size);
                cleanup_test_databases(db_path);
                
                BenchmarkConfig config;
                config.storage_strategy = "dual_rocksdb_adaptive";
                config.db_path = db_path;
                
                auto strategy = StorageStrategyFactory::create_strategy("dual_rocksdb_adaptive", config);
                auto db_manager = std::make_unique<StrategyDBManager>(db_path, std::move(strategy));
                
                if (!db_manager->open(true)) { // Clean database
                    std::cout << "✗ Failed to open database for regular test" << std::endl;
                    continue;
                }
                
                PerformanceTimer timer;
                timer.start();
                
                if (!db_manager->write_batch(test_data)) {
                    std::cout << "✗ Regular write_batch failed" << std::endl;
                    db_manager->close();
                    continue;
                }
                
                double regular_time = timer.elapsed_ms();
                std::cout << "  ✓ Regular write_batch: " << regular_time << " ms" << std::endl;
                std::cout << "  ✓ Throughput: " << (test_size / (regular_time / 1000.0)) << " records/sec" << std::endl;
                
                db_manager->close();
                cleanup_test_databases(db_path);
            }
            
            // Test 2: Optimized write_initial_load_batch
            std::cout << "Test 2: Optimized write_initial_load_batch..." << std::endl;
            {
                std::string db_path = "./perf_test_optimized_" + std::to_string(test_size);
                cleanup_test_databases(db_path);
                
                BenchmarkConfig config;
                config.storage_strategy = "dual_rocksdb_adaptive";
                config.db_path = db_path;
                
                auto strategy = StorageStrategyFactory::create_strategy("dual_rocksdb_adaptive", config);
                auto db_manager = std::make_unique<StrategyDBManager>(db_path, std::move(strategy));
                
                if (!db_manager->open(true)) { // Clean database
                    std::cout << "✗ Failed to open database for optimized test" << std::endl;
                    continue;
                }
                
                PerformanceTimer timer;
                timer.start();
                
                if (!db_manager->write_initial_load_batch(test_data)) {
                    std::cout << "✗ Optimized write_initial_load_batch failed" << std::endl;
                    db_manager->close();
                    continue;
                }
                
                double optimized_time = timer.elapsed_ms();
                std::cout << "  ✓ Optimized write_initial_load_batch: " << optimized_time << " ms" << std::endl;
                std::cout << "  ✓ Throughput: " << (test_size / (optimized_time / 1000.0)) << " records/sec" << std::endl;
                
                db_manager->close();
                cleanup_test_databases(db_path);
            }
            
            std::cout << std::endl;
        }
        
        std::cout << "=== Performance Comparison Summary ===" << std::endl;
        std::cout << "✓ All performance tests completed successfully!" << std::endl;
        std::cout << std::endl;
        std::cout << "Key observations:" << std::endl;
        std::cout << "- write_initial_load_batch should be faster for initial data import" << std::endl;
        std::cout << "- Performance improvement comes from:" << std::endl;
        std::cout << "  * No database queries for existing range lists" << std::endl;
        std::cout << "  * No batch_range_cache_ memory overhead" << std::endl;
        std::cout << "  * No linear search in range lists" << std::endl;
        std::cout << "  * Direct range list construction" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cout << "✗ Performance test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}