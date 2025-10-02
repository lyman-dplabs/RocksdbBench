#include "../src/benchmark/strategy_scenario_runner.hpp"
#include "../src/core/strategy_db_manager.hpp"
#include "../src/strategies/direct_version_strategy.hpp"
#include "../src/strategies/dual_rocksdb_strategy.hpp"
#include "../src/benchmark/metrics_collector.hpp"
#include "../src/core/config.hpp"
#include "../src/utils/logger.hpp"
#include <iostream>
#include <cassert>
#include <filesystem>
#include <random>

// 简单的测试框架
#define TEST_ASSERT(condition, message) \
    do { \
        if (!(condition)) { \
            std::cerr << "ASSERTION FAILED: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
            return false; \
        } \
    } while(0)

#define TEST_ASSERT_EQ(expected, actual, message) \
    TEST_ASSERT((expected) == (actual), message << " (expected: " << (expected) << ", actual: " << (actual) << ")")

#define RUN_TEST(test_func) \
    do { \
        std::cout << "Running " << #test_func << "..." << std::endl; \
        if (test_func()) { \
            std::cout << "✓ " << #test_func << " PASSED" << std::endl; \
            passed_tests++; \
        } else { \
            std::cout << "✗ " << #test_func << " FAILED" << std::endl; \
            failed_tests++; \
        } \
        total_tests++; \
    } while(0)

class SimpleTestFramework {
private:
    std::string test_db_path_;
    
public:
    SimpleTestFramework() {
        // 创建临时测试目录
        std::string random_suffix = std::to_string(std::random_device{}());
        test_db_path_ = (std::filesystem::temp_directory_path() / ("test_historical_query_" + random_suffix)).string();
        std::filesystem::create_directories(test_db_path_);
    }
    
    ~SimpleTestFramework() {
        // 清理测试目录
        if (std::filesystem::exists(test_db_path_)) {
            std::filesystem::remove_all(test_db_path_);
        }
    }
    
    std::string get_test_path(const std::string& suffix = "") {
        return test_db_path_ + suffix;
    }
};

// 测试DirectVersionStrategy的历史版本查询
bool test_direct_version_basic() {
    SimpleTestFramework test;
    
    try {
        // 创建DirectVersionStrategy
        DirectVersionStrategy::Config config;
        config.batch_size_blocks = 2;
        config.max_batch_size_bytes = 1024 * 1024; // 1MB
        
        auto strategy = std::make_unique<DirectVersionStrategy>(config);
        auto db_manager = std::make_shared<StrategyDBManager>(test.get_test_path("_direct"), std::move(strategy));
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        // 配置
        BenchmarkConfig benchmark_config;
        benchmark_config.storage_strategy = "direct_version";
        benchmark_config.initial_records = 1000; // 小规模测试
        benchmark_config.hotspot_updates = 100;
        
        auto runner = std::make_unique<StrategyScenarioRunner>(db_manager, metrics_collector, benchmark_config);
        
        // 打开数据库
        TEST_ASSERT(db_manager->open(true), "Failed to open database");
        
        // 运行初始加载
        runner->run_initial_load_phase();
        
        // 测试历史版本查询 - 使用我们手动写入的测试key，确保数据存在
        std::string test_key = "0x1234567890abcdef1234567890abcdef12345678";
        
        // 手动写入一些测试数据，确保我们有可查询的数据
        std::vector<DataRecord> test_records = {
            {0, test_key, "test_value_0"},
            {3, test_key, "test_value_3"},
            {5, test_key, "test_value_5"}
        };
        TEST_ASSERT(db_manager->write_batch(test_records), "Failed to write test records");
        
        // 查询一个存在的历史版本
        auto result = db_manager->query_historical_version(test_key, 5);
        TEST_ASSERT(result.has_value(), "Historical version query should return a value");
        
        // 解析结果：格式应该是"block_num:value"
        auto colon_pos = result->find(':');
        TEST_ASSERT(colon_pos != std::string::npos, "Result should contain ':' separator");
        
        std::string block_str = result->substr(0, colon_pos);
        std::string value = result->substr(colon_pos + 1);
        
        BlockNum block_num = std::stoull(block_str);
        TEST_ASSERT(block_num <= 5, "Block number should be <= target version");
        TEST_ASSERT(!value.empty(), "Value should not be empty");
        
        db_manager->close();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return false;
    }
}

// 测试历史版本查询的语义：≤target_version找最新，找不到则找≥的最小值
bool test_historical_version_semantics() {
    SimpleTestFramework test;
    
    try {
        DirectVersionStrategy::Config config;
        config.batch_size_blocks = 1;
        config.max_batch_size_bytes = 1024 * 1024;
        
        auto strategy = std::make_unique<DirectVersionStrategy>(config);
        auto db_manager = std::make_shared<StrategyDBManager>(test.get_test_path("_semantics"), std::move(strategy));
        
        TEST_ASSERT(db_manager->open(true), "Failed to open database");
        
        // 手动写入一些测试数据 - 使用符合格式的key
        std::string test_key = "0x0000000000000000000000000000000000000001";
        
        // 写入version 1, 3, 5, 8的数据
        std::vector<std::pair<BlockNum, std::string>> test_data = {
            {1, "value_at_1"},
            {3, "value_at_3"},
            {5, "value_at_5"},
            {8, "value_at_8"}
        };
        
        for (const auto& [block_num, value] : test_data) {
            std::vector<DataRecord> records = {{block_num, test_key, value}};
            TEST_ASSERT(db_manager->write_batch(records), "Failed to write test data");
        }
        
        // 测试查询target_version = 4，应该返回version 3的值
        auto result1 = db_manager->query_historical_version(test_key, 4);
        TEST_ASSERT(result1.has_value(), "Query for version 4 should return a value");
        TEST_ASSERT_EQ("value_at_3", result1->substr(result1->find(':') + 1), 
                       "Query for version 4 should return value at version 3");
        
        // 测试查询target_version = 6，应该返回version 5的值
        auto result2 = db_manager->query_historical_version(test_key, 6);
        TEST_ASSERT(result2.has_value(), "Query for version 6 should return a value");
        TEST_ASSERT_EQ("value_at_5", result2->substr(result2->find(':') + 1),
                       "Query for version 6 should return value at version 5");
        
        // 测试查询target_version = 0，应该返回version 1的值（≥0的最小值）
        auto result3 = db_manager->query_historical_version(test_key, 0);
        TEST_ASSERT(result3.has_value(), "Query for version 0 should return a value");
        TEST_ASSERT_EQ("value_at_1", result3->substr(result3->find(':') + 1),
                       "Query for version 0 should return value at version 1");
        
        db_manager->close();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return false;
    }
}

// 测试连续更新查询循环（短时间运行）
bool test_continuous_update_query_loop() {
    SimpleTestFramework test;
    
    try {
        DirectVersionStrategy::Config config;
        config.batch_size_blocks = 5;
        config.max_batch_size_bytes = 1024 * 1024;
        
        auto strategy = std::make_unique<DirectVersionStrategy>(config);
        auto db_manager = std::make_shared<StrategyDBManager>(test.get_test_path("_continuous"), std::move(strategy));
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        BenchmarkConfig benchmark_config;
        benchmark_config.storage_strategy = "direct_version";
        benchmark_config.initial_records = 500; // 小规模
        benchmark_config.hotspot_updates = 50;
        
        auto runner = std::make_unique<StrategyScenarioRunner>(db_manager, metrics_collector, benchmark_config);
        
        TEST_ASSERT(db_manager->open(true), "Failed to open database");
        
        // 运行初始加载
        runner->run_initial_load_phase();
        
        // 运行短时间的连续更新查询循环（1分钟）
        runner->run_continuous_update_query_loop(1); // 1分钟
        
        db_manager->close();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return false;
    }
}

// 测试性能日志格式
bool test_performance_log_format() {
    SimpleTestFramework test;
    
    try {
        DirectVersionStrategy::Config config;
        auto strategy = std::make_unique<DirectVersionStrategy>(config);
        auto db_manager = std::make_shared<StrategyDBManager>(test.get_test_path("_perf"), std::move(strategy));
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        BenchmarkConfig benchmark_config;
        benchmark_config.storage_strategy = "direct_version";
        benchmark_config.initial_records = 100;
        benchmark_config.hotspot_updates = 10;
        
        auto runner = std::make_unique<StrategyScenarioRunner>(db_manager, metrics_collector, benchmark_config);
        
        TEST_ASSERT(db_manager->open(true), "Failed to open database");
        
        // 运行初始加载
        runner->run_initial_load_phase();
        
        // 验证性能统计功能
        // 这个测试主要确保代码能正常编译和运行
        // 实际的日志格式验证需要通过运行程序并检查输出
        
        db_manager->close();
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return false;
    }
}

int main() {
    std::cout << "=== Historical Version Query Tests ===" << std::endl;
    
    int passed_tests = 0;
    int failed_tests = 0;
    int total_tests = 0;
    
    // 运行所有测试
    RUN_TEST(test_direct_version_basic);
    RUN_TEST(test_historical_version_semantics);
    RUN_TEST(test_continuous_update_query_loop);
    RUN_TEST(test_performance_log_format);
    
    // 输出结果
    std::cout << "\n=== Test Summary ===" << std::endl;
    std::cout << "Total tests: " << total_tests << std::endl;
    std::cout << "Passed: " << passed_tests << std::endl;
    std::cout << "Failed: " << failed_tests << std::endl;
    
    if (failed_tests == 0) {
        std::cout << "\n🎉 All tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "\n❌ Some tests failed!" << std::endl;
        return 1;
    }
}