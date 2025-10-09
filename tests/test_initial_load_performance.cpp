#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <thread>
#include <random>
#include <filesystem>
#include <CLI/CLI.hpp>

#include "../core/strategy_db_manager.hpp"
#include "../strategies/dual_rocksdb_strategy.hpp"
#include "../benchmark/strategy_scenario_runner.hpp"
#include "../benchmark/metrics_collector.hpp"
#include "../utils/data_generator.hpp"
#include "../utils/logger.hpp"
#include "../core/types.hpp"
#include "../core/config.hpp"

using BlockNum = uint64_t;
using Value = std::string;

int main(int argc, char** argv) {
    CLI::App app{"Initial Load Performance Test - 1B Keys"};
    
    std::string db_path = "./rocksdb_data_initial_load_test";
    size_t total_keys = 100000000;  // 1亿条key
    bool verbose = false;
    
    app.add_option("-p,--db-path", db_path, "Database path")
        ->default_val("./rocksdb_data_initial_load_test");
    
    app.add_option("-k,--total-keys", total_keys, "Total number of keys to generate")
        ->default_val(100000000)
        ->check(CLI::PositiveNumber);
    
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");
    
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        std::cerr << "Parse error: " << e.what() << std::endl;
        return app.exit(e);
    }
    
    std::cout << "=== Initial Load Performance Test ===" << std::endl;
    std::cout << "Database path: " << db_path << std::endl;
    std::cout << "Total keys: " << total_keys << std::endl;
    std::cout << "=====================================" << std::endl;
    
    try {
        // 清理现有数据库（如果存在）
        if (std::filesystem::exists(db_path)) {
            std::cout << "Cleaning existing database..." << std::endl;
            std::filesystem::remove_all(db_path);
        }
        
        // 清理DualRocksDB的两个数据库
        std::string range_db_path = db_path + "_range_index";
        std::string data_db_path = db_path + "_data_storage";
        
        if (std::filesystem::exists(range_db_path)) {
            std::filesystem::remove_all(range_db_path);
        }
        if (std::filesystem::exists(data_db_path)) {
            std::filesystem::remove_all(data_db_path);
        }
        
        std::cout << "Database cleanup completed" << std::endl;
        
        // 创建DualRocksDB策略配置
        DualRocksDBStrategy::Config strategy_config;
        strategy_config.range_size = 10000;
        strategy_config.enable_dynamic_cache_optimization = false;  // 使用直接查询模式
        strategy_config.batch_size_blocks = 10;  // 每10个block刷写一次
        strategy_config.max_batch_size_bytes = 1024 * 1024 * 1024; // 1GB
        
        // 创建DualRocksDB策略
        auto strategy = std::make_unique<DualRocksDBStrategy>(strategy_config);
        
        // 创建数据库管理器
        auto db_manager = std::make_shared<StrategyDBManager>(db_path, std::move(strategy));
        
        std::cout << "Opening database manager..." << std::endl;
        
        if (!db_manager->open(true)) {  // force_clean = true
            throw std::runtime_error("Failed to open database manager");
        }
        
        std::cout << "Database manager initialized successfully" << std::endl;
        
        // 创建Benchmark配置
        BenchmarkConfig benchmark_config;
        benchmark_config.total_keys = total_keys;
        benchmark_config.range_size = 10000;
        benchmark_config.enable_dynamic_cache_optimization = false;
        
        // 创建指标收集器
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        // 创建DataGenerator配置
        DataGenerator::Config data_config;
        data_config.total_keys = total_keys;
        data_config.hotspot_count = static_cast<size_t>(total_keys * 0.1);  // 10% hot keys
        data_config.medium_count = static_cast<size_t>(total_keys * 0.2);  // 20% medium keys
        data_config.tail_count = total_keys - data_config.hotspot_count - data_config.medium_count;  // 70% tail keys
        
        std::cout << "Creating DataGenerator..." << std::endl;
        std::cout << "  Total keys: " << data_config.total_keys << std::endl;
        std::cout << "  Hot/Medium/Tail keys: " << data_config.hotspot_count << "/" << data_config.medium_count << "/" << data_config.tail_count << std::endl;
        
        auto data_generator = std::make_unique<DataGenerator>(data_config);
        
        // 创建ScenarioRunner
        auto scenario_runner = std::make_unique<StrategyScenarioRunner>(db_manager, metrics_collector, benchmark_config, std::move(data_generator));
        
        std::cout << "Starting initial load phase..." << std::endl;
        std::cout << "This may take a while for 1B keys..." << std::endl;
        
        // 记录开始时间
        auto start_time = std::chrono::steady_clock::now();
        
        // 执行 initial load phase
        scenario_runner->run_initial_load_phase();
        
        // 记录结束时间
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
        
        std::cout << "\n=== Initial Load Completed ===" << std::endl;
        std::cout << "Total duration: " << duration.count() << " seconds";
        std::cout << " (" << (duration.count() / 60) << " minutes)" << std::endl;
        
        // 计算吞吐量
        double throughput = static_cast<double>(total_keys) / duration.count();
        std::cout << "Throughput: " << static_cast<long>(throughput) << " keys/second" << std::endl;
        
        // 获取数据库统计信息
        scenario_runner->collect_rocksdb_statistics();
        
        // 清理数据库
        std::cout << "\nCleaning up databases..." << std::endl;
        db_manager.reset();  // 确保数据库正确关闭
        
        std::filesystem::remove_all(db_path);
        std::filesystem::remove_all(range_db_path);
        std::filesystem::remove_all(data_db_path);
        
        std::cout << "Test completed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}