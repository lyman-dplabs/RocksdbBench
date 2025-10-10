#pragma once
#include "../core/strategy_db_manager.hpp"
#include "metrics_collector.hpp"
#include "../utils/data_generator.hpp"
#include "../core/config.hpp"
#include <memory>
#include <chrono>
#include <vector>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>

class StrategyScenarioRunner {
public:
    // 并发测试配置
    struct ConcurrentTestConfig {
        size_t reader_thread_count = 10;      // 读线程数量
        size_t queries_per_thread = 200;       // 每个读线程的查询次数
        size_t test_duration_seconds = 3600;   // 测试持续时间（秒）
        size_t write_sleep_seconds = 3;        // 写线程sleep时间
        size_t block_size = 10000;             // 每个block的kv数量

        // 获取推荐的读线程数量（CPU核心数的2倍）
        static size_t get_recommended_reader_threads() {
            unsigned int cpu_cores = std::thread::hardware_concurrency();

            // 限制最大线程数以避免过度并发
            size_t thread_count;
            if (cpu_cores == 0) {
                // 如果无法获取CPU核心数，使用默认值
                thread_count = 16;  // 假设8核CPU
            } else if (cpu_cores <= 4) {
                thread_count = cpu_cores * 4;  // 小核心数机器，4倍线程
            } else if (cpu_cores <= 8) {
                thread_count = cpu_cores * 3;  // 中等核心数机器，3倍线程
            } else {
                thread_count = cpu_cores * 2;  // 大核心数机器，2倍线程
            }

            // 设置合理的上限，避免过多线程
            const size_t max_threads = 32;
            if (thread_count > max_threads) {
                thread_count = max_threads;
            }

            return thread_count;
        }

        // 构造函数，从BenchmarkConfig转换
        static ConcurrentTestConfig from_benchmark_config(const BenchmarkConfig& config) {
            ConcurrentTestConfig test_config;
            // 注意：reader_thread_count 现在不再使用，实际线程数由 get_recommended_reader_threads() 动态计算
            test_config.reader_thread_count = 0; // 这个值现在被忽略
            test_config.test_duration_seconds = config.continuous_duration_minutes * 60;
            return test_config;
        }
    };

    StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager,
                         std::shared_ptr<MetricsCollector> metrics,
                         const BenchmarkConfig& config);
  
  // 新的构造函数：支持外部DataGenerator（用于recovery test）
  StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager,
                         std::shared_ptr<MetricsCollector> metrics,
                         const BenchmarkConfig& config,
                         std::unique_ptr<DataGenerator> external_data_generator,
                         size_t initial_load_end_block,
                         size_t max_block);

    void run_initial_load_phase();

    // 新的并发读写测试接口
    void run_concurrent_read_write_test(const ConcurrentTestConfig& test_config);

    // 兼容性接口 - 从旧的continuous_duration_minutes转换
    void run_continuous_update_query_loop(size_t duration_minutes = 360);

    // Collect real RocksDB statistics
    void collect_rocksdb_statistics();

    // Get current strategy information
    std::string get_current_strategy() const;

    // 获取性能统计结果
    struct PerformanceStats {
        size_t total_write_ops = 0;
        size_t total_query_ops = 0;
        size_t successful_queries = 0;
        double test_duration_seconds = 0.0;

        // 查询性能
        std::vector<double> query_latencies_ms;
        double query_avg_ms = 0.0;
        double query_p50_ms = 0.0;
        double query_p95_ms = 0.0;
        double query_p99_ms = 0.0;
        double query_min_ms = 0.0;
        double query_max_ms = 0.0;
        double query_ops_per_sec = 0.0;
        double query_success_rate = 0.0;

        // 写入性能
        std::vector<double> write_latencies_ms;
        double write_avg_ms = 0.0;
        double write_p50_ms = 0.0;
        double write_p95_ms = 0.0;
        double write_p99_ms = 0.0;
        double write_ops_per_sec = 0.0;

        void print_statistics() const;
    };

    PerformanceStats get_performance_stats() const;

    // Test support methods for accessing internal mutexes
    std::mutex& get_write_perf_mutex() { return write_perf_mutex_; }
    std::mutex& get_query_merge_mutex() { return query_merge_mutex_; }

private:
    std::shared_ptr<StrategyDBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    std::unique_ptr<DataGenerator> data_generator_;
    BenchmarkConfig config_;

    // 测试状态
    BlockNum initial_load_end_block_ = 0;
    std::atomic<BlockNum> current_max_block_{0};
    std::atomic<bool> test_running_{false};

    // 优化后的并发控制和性能统计

    // 写线程专用锁和数据
    mutable std::mutex write_perf_mutex_;
    std::vector<double> write_latencies_;
    std::atomic<size_t> write_count_{0};

    // 读线程使用线程本地存储
    thread_local static std::vector<double> thread_query_latencies_;
    std::atomic<size_t> total_successful_queries_{0};

    // 合并后的最终查询延迟数据（只写一次）
    std::vector<double> query_latencies_;
    mutable std::mutex query_merge_mutex_;

    // 状态保护
    mutable std::mutex state_mutex_;

    // 写线程函数
    void writer_thread_function(size_t duration_seconds, size_t sleep_seconds, size_t block_size);

    // 读线程函数
    void reader_thread_function(int thread_id, std::chrono::seconds test_duration);

    // 性能统计计算
    void calculate_performance_statistics(PerformanceStats& stats) const;

    // 兼容性：保留旧的查询接口
    struct QueryResult {
        bool found;
        BlockNum block_num;
        Value value;
        double latency_ms;
    };

    QueryResult query_historical_version(const std::string& addr_slot, BlockNum target_version);
};