#include <iostream>
#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <thread>
#include <random>
#include <mutex>
#include <atomic>
#include <algorithm>
#include <ctime>
#include <stdexcept>
#include <filesystem>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
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

// 从Range Index DB提取所有地址（支持大数据库的分批处理）
std::vector<std::string> extract_addresses_from_range_db(rocksdb::DB* range_db) {
    std::cout << "Extracting addresses from Range Index DB..." << std::endl;
    
    std::vector<std::string> addresses;
    addresses.reserve(20000000000ULL);
    rocksdb::Iterator* it = range_db->NewIterator(rocksdb::ReadOptions());
    
    size_t count = 0;
    size_t batch_size = 100000000;  // 每批处理1亿个地址，避免内存问题
    
    try {
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string key = it->key().ToString();
            if (!key.empty()) {
                addresses.push_back(key);
                count++;
                
                // 每处理1亿个地址打印一次进度
                if (count % batch_size == 0) {
                    std::cout << "  Extracted " << count << " addresses..." << std::endl;
                    
                    // 预分配内存以提高性能
                    addresses.reserve(addresses.size() + batch_size);
                }
            }
            
            // 安全检查：避免无限循环
            if (count > 10000000000ULL) {  // 最多100亿地址
                std::cout << "Warning: Reached maximum address limit (10B), stopping extraction." << std::endl;
                break;
            }
        }
    } catch (const std::exception& e) {
        delete it;
        throw std::runtime_error("Exception during address extraction: " + std::string(e.what()));
    }
    
    rocksdb::Status status = it->status();
    delete it;
    
    if (!status.ok()) {
        throw std::runtime_error("Error iterating Range Index DB: " + status.ToString());
    }
    
    std::cout << "Successfully extracted " << addresses.size() << " addresses" << std::endl;
    std::cout << "Memory usage estimate: " << (addresses.size() * 85 / 1024 / 1024) << " MB" << std::endl;
    
    return addresses;
}

// 从Data Storage DB获取最大block number
BlockNum find_max_block_from_data_db(rocksdb::DB* data_db) {
    std::cout << "Finding maximum block number from Data Storage DB..." << std::endl;
    
    BlockNum max_block = 0;
    rocksdb::Iterator* it = data_db->NewIterator(rocksdb::ReadOptions());
    
    // 从最后一条记录开始查找
    it->SeekToLast();
    
    if (it->Valid()) {
        std::string key = it->key().ToString();
        
        // 解析key格式: R{range_num}|{addr_slot}|{block_number}
        if (key.rfind("R", 0) == 0) {  // key以'R'开头
            size_t last_pipe = key.find_last_of('|');
            if (last_pipe != std::string::npos && last_pipe < key.length() - 1) {
                std::string block_str = key.substr(last_pipe + 1);
                try {
                    max_block = std::stoull(block_str);
                    std::cout << "Found max block: " << max_block << std::endl;
                    std::cout << "Sample key: " << key << std::endl;
                } catch (const std::exception& e) {
                    std::cerr << "Error parsing block number from key: " << key << std::endl;
                    std::cerr << "Error: " << e.what() << std::endl;
                }
            }
        }
    }
    
    delete it;
    
    if (max_block == 0) {
        throw std::runtime_error("Could not find any valid block number in Data Storage DB");
    }
    
    return max_block;
}

// 打开现有数据库（优化大数据库打开性能）
rocksdb::DB* open_existing_db(const std::string& db_path) {
    std::cout << "Opening database at: " << db_path << std::endl;
    
    // 获取数据库大小信息
    std::uintmax_t db_size = 0;
    if (std::filesystem::exists(db_path)) {
        for (const auto& entry : std::filesystem::recursive_directory_iterator(db_path)) {
            if (entry.is_regular_file()) {
                db_size += entry.file_size();
            }
        }
    }
    
    std::cout << "Database size: " << (db_size / 1024 / 1024 / 1024) << " GB" << std::endl;
    std::cout << "This may take a while for large databases..." << std::endl;
    
    rocksdb::Options options;
    options.create_if_missing = false;
    options.use_direct_io_for_flush_and_compaction = true;
    
    // 大数据库优化配置
    options.max_background_compactions = 16;
    options.max_background_flushes = 8;
    options.max_open_files = -1;  // 不限制文件句柄数量
    options.skip_stats_update_on_db_open = true;  // 跳过统计更新，加快打开速度
    options.allow_mmap_reads = true;  // 允许mmap读取，可能提高大文件性能
    
    // 显示进度
    auto start_time = std::chrono::steady_clock::now();
    std::cout << "Starting RocksDB open process..." << std::endl;
    
    rocksdb::DB* db;
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db);
    
    auto end_time = std::chrono::steady_clock::now();
    auto open_duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
    
    if (!status.ok()) {
        std::cerr << "Failed to open database after " << open_duration.count() << " seconds" << std::endl;
        throw std::runtime_error("Failed to open database at " + db_path + ": " + status.ToString());
    }
    
    std::cout << "Successfully opened database: " << db_path << " in " << open_duration.count() << " seconds" << std::endl;
    return db;
}


// 运行并发测试 - 直接使用现有的DualRocksDB Strategy和ScenarioRunner
void run_concurrent_test_with_recovered_data(
    rocksdb::DB* range_db,
    rocksdb::DB* data_db,
    std::vector<std::string> addresses,
    BlockNum max_block,
    size_t duration_seconds) {
    
    std::cout << "\n=== Starting Concurrent Read-Write Test with Recovered Data ===" << std::endl;
    std::cout << "Test duration: " << duration_seconds << " seconds" << std::endl;
    std::cout << "Addresses: " << addresses.size() << std::endl;
    std::cout << "Max block: " << max_block << std::endl;
    
    // 创建临时主数据库路径，用于DualRocksDB策略初始化
    std::string temp_main_db_path = "/tmp/temp_dual_main_db_" + std::to_string(std::time(nullptr));
    
    // 创建DualRocksDB策略配置
    DualRocksDBStrategy::Config strategy_config;
    strategy_config.range_size = 10000;
    strategy_config.enable_dynamic_cache_optimization = false;  // 使用直接查询模式
    strategy_config.batch_size_blocks = 5;
    strategy_config.max_batch_size_bytes = 128 * 1024 * 1024; // 128MB
    
    // 创建DualRocksDB策略
    auto strategy = std::make_unique<DualRocksDBStrategy>(strategy_config);
    
    std::cout << "Creating temporary main database at: " << temp_main_db_path << std::endl;
    
    // 创建数据库管理器
    auto db_manager = std::make_shared<StrategyDBManager>(temp_main_db_path, std::move(strategy));
    
    if (!db_manager->open(true)) {  // force_clean = true
        rocksdb::DestroyDB(temp_main_db_path, rocksdb::Options());
        throw std::runtime_error("Failed to open database manager");
    }
    
    std::cout << "Database manager initialized successfully" << std::endl;
    
    // 创建Benchmark配置
    BenchmarkConfig benchmark_config;
    benchmark_config.continuous_duration_minutes = duration_seconds / 60;
    benchmark_config.total_keys = addresses.size();
    benchmark_config.range_size = 10000;
    benchmark_config.enable_dynamic_cache_optimization = false;
    
    // 创建指标收集器
    auto metrics_collector = std::make_shared<MetricsCollector>();
    
    DataGenerator::Config data_config;
    data_config.total_keys = addresses.size();
    data_config.hotspot_count = static_cast<size_t>(addresses.size() * 0.8);  // 80% hot keys
    data_config.medium_count = static_cast<size_t>(addresses.size() * 0.1);   // 10% medium keys
    data_config.tail_count = addresses.size() - data_config.hotspot_count - data_config.medium_count;  // 10% tail keys
    
    auto external_data_generator = std::make_unique<DataGenerator>(std::move(addresses), data_config);
    
    std::cout << "Created DataGenerator with recovered keys:" << std::endl;
    std::cout << "  Total keys: " << external_data_generator->get_all_keys().size() << std::endl;
    std::cout << "  Hot/Medium/Tail keys: " << data_config.hotspot_count << "/" << data_config.medium_count << "/" << data_config.tail_count << std::endl;
    
    // 创建ScenarioRunner，使用外部DataGenerator
    auto scenario_runner = std::make_unique<StrategyScenarioRunner>(db_manager, metrics_collector, benchmark_config, std::move(external_data_generator));
    
    // 创建测试配置
    StrategyScenarioRunner::ConcurrentTestConfig test_config;
    test_config.reader_thread_count = StrategyScenarioRunner::ConcurrentTestConfig::get_recommended_reader_threads();
    test_config.test_duration_seconds = duration_seconds;
    test_config.block_size = 10000;
    test_config.write_sleep_seconds = 3;
    
    std::cout << "Test configuration:" << std::endl;
    std::cout << "  Reader threads: " << test_config.reader_thread_count << std::endl;
    std::cout << "  Block size: " << test_config.block_size << std::endl;
    std::cout << "  Write sleep: " << test_config.write_sleep_seconds << "s" << std::endl;
    std::cout << "  Strategy: Dual RocksDB Adaptive" << std::endl;
    std::cout << "  Note: Using recovered data pattern for test simulation" << std::endl;
    
    std::cout << "\n=== Starting Concurrent Test ===" << std::endl;
    
    // 运行并发测试
    auto start_time = std::chrono::steady_clock::now();
    
    try {
        scenario_runner->run_concurrent_read_write_test(test_config);
    } catch (const std::exception& e) {
        std::cerr << "Error during concurrent test: " << e.what() << std::endl;
        // 清理临时数据库
        rocksdb::DestroyDB(temp_main_db_path, rocksdb::Options());
        throw;
    }
    
    auto end_time = std::chrono::steady_clock::now();
    auto actual_duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
    
    std::cout << "\n=== Test Completed ===" << std::endl;
    std::cout << "Actual duration: " << actual_duration.count() << " seconds" << std::endl;
    
    // 获取并打印性能统计
    auto performance_stats = scenario_runner->get_performance_stats();
    performance_stats.print_statistics();
    
    // 收集RocksDB统计信息
    scenario_runner->collect_rocksdb_statistics();
    
    // 清理临时数据库
    rocksdb::DestroyDB(temp_main_db_path, rocksdb::Options());
    
    std::cout << "Concurrent test completed successfully!" << std::endl;
    std::cout << "Note: Test used Dual RocksDB strategy with simulated workload based on recovered data characteristics" << std::endl;
}

int main(int argc, char** argv) {
    CLI::App app{"Dual RocksDB 2B Recovery Test - Test with existing 2B database"};
    
    std::string range_db_path;
    std::string data_db_path;
    size_t duration_seconds = 900;  // 默认15分钟
    
    app.add_option("-r,--range-db", range_db_path, "Path to range index database")
        ->required();
    
    app.add_option("-d,--data-db", data_db_path, "Path to data storage database")
        ->required();
        
    app.add_option("-t,--duration", duration_seconds, "Test duration in seconds (default: 900)")
        ->check(CLI::PositiveNumber);
    
    bool verbose = false;
    app.add_flag("-v,--verbose", verbose, "Enable verbose output");
    
    try {
        app.parse(argc, argv);
    } catch (const CLI::ParseError& e) {
        std::cerr << "Parse error: " << e.what() << std::endl;
        return app.exit(e);
    }
    
    std::cout << "=== Dual RocksDB 2B Recovery Test ===" << std::endl;
    std::cout << "Range DB path: " << range_db_path << std::endl;
    std::cout << "Data DB path: " << data_db_path << std::endl;
    std::cout << "Test duration: " << duration_seconds << " seconds (" << (duration_seconds / 60) << " minutes)" << std::endl;
    std::cout << "======================================" << std::endl;
    
    try {
        // 1. 打开现有数据库
        rocksdb::DB* range_db = open_existing_db(range_db_path);
        rocksdb::DB* data_db = open_existing_db(data_db_path);
        
        // 2. 提取数据
        auto addresses = extract_addresses_from_range_db(range_db);
        BlockNum max_block = find_max_block_from_data_db(data_db);
        
        // 3. 运行测试
        auto start_time = std::chrono::steady_clock::now();
        run_concurrent_test_with_recovered_data(range_db, data_db, addresses, max_block, duration_seconds);
        auto end_time = std::chrono::steady_clock::now();
        
        auto actual_duration = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time);
        std::cout << "\n=== Test Completed ===" << std::endl;
        std::cout << "Actual duration: " << actual_duration.count() << " seconds" << std::endl;
        
        // 4. 清理资源
        delete range_db;
        delete data_db;
        
        std::cout << "Test completed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}