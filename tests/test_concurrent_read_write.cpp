#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <random>
#include <memory>
#include <mutex>
#include <iostream>
#include "../src/core/strategy_db_manager.hpp"
#include "../src/benchmark/metrics_collector.hpp"
#include "../src/core/types.hpp"
#include "../src/core/config.hpp"
#include "../src/utils/data_generator.hpp"
#include "../src/utils/logger.hpp"
#include "../src/strategies/direct_version_strategy.hpp"
#include <numeric>

using namespace utils;

// 并发读写测试类
class ConcurrentReadWriteTest {
public:
    ConcurrentReadWriteTest() {
        // 设置测试配置
        config_.total_keys = 100000;  // 10万keys用于测试
        config_.continuous_duration_minutes = 1;  // 1分钟测试
        config_.clean_existing_data = true;
        config_.cache_size = 64 * 1024 * 1024;  // 64MB cache
        config_.verbose = true;

        // 创建临时数据库路径
        test_db_path_ = "./test_concurrent_rw_data";
        config_.db_path = test_db_path_;

        // 初始化组件
        metrics_collector_ = std::make_shared<MetricsCollector>();

        // 创建存储策略（默认使用direct version）
        DirectVersionStrategy::Config strategy_config;
        strategy_config.batch_size_blocks = 5;
        strategy_config.max_batch_size_bytes = 4UL * 1024 * 1024 * 1024;

        std::unique_ptr<IStorageStrategy> strategy = std::make_unique<DirectVersionStrategy>(strategy_config);
        db_manager_ = std::make_shared<StrategyDBManager>(test_db_path_, std::move(strategy));

        // 打开数据库
        if (!db_manager_->open(config_.clean_existing_data)) {
            throw std::runtime_error("Failed to open database");
        }

        // 初始化数据生成器
        DataGenerator::Config data_config;
        data_config.total_keys = config_.total_keys;
        data_config.hotspot_count = static_cast<size_t>(config_.total_keys * 0.1);
        data_config.medium_count = static_cast<size_t>(config_.total_keys * 0.2);
        data_config.tail_count = config_.total_keys - data_config.hotspot_count - data_config.medium_count;

        data_generator_ = std::make_unique<DataGenerator>(data_config);

        utils::log_info("ConcurrentReadWriteTest initialized with {} keys", config_.total_keys);
    }

    ~ConcurrentReadWriteTest() {
        // 清理测试数据
        db_manager_.reset();
        metrics_collector_.reset();
        data_generator_.reset();

        // 删除测试数据库文件
        std::string cleanup_cmd = "rm -rf " + test_db_path_;
        system(cleanup_cmd.c_str());

        utils::log_info("ConcurrentReadWriteTest cleanup completed");
    }

    // 执行初始加载
    void run_initial_load() {
        utils::log_info("=== Starting Initial Load Phase ===");

        const auto& all_keys = data_generator_->get_all_keys();
        const size_t batch_size = 10000;
        size_t total_keys = all_keys.size();
        BlockNum current_block = 0;

        for (size_t i = 0; i < total_keys; i += batch_size) {
            size_t end_idx = std::min(i + batch_size, total_keys);
            size_t current_batch_size = end_idx - i;

            std::vector<DataRecord> records;
            auto random_values = data_generator_->generate_random_values(current_batch_size);

            records.reserve(current_batch_size);

            for (size_t j = 0; j < current_batch_size; ++j) {
                size_t key_idx = i + j;
                DataRecord record{
                    current_block,
                    all_keys[key_idx],
                    random_values[j]
                };
                records.push_back(record);
            }

            bool success = db_manager_->write_initial_load_batch(records);
            if (!success) {
                utils::log_error("Failed to write batch at block {}", current_block);
                throw std::runtime_error("Initial load failed");
            }

            current_block++;

            if (i % 20000 == 0) {
                utils::log_info("Initial load progress: {}/{} ({:.1f}%)",
                               i, total_keys, (i * 100.0 / total_keys));
            }
        }

        db_manager_->flush_all_batches();
        initial_load_end_block_ = current_block;
        current_max_block_ = current_block - 1;

        utils::log_info("=== Initial Load Completed ===");
        utils::log_info("Total blocks written: {}, keys tracked: {}",
                       initial_load_end_block_, total_keys);
    }

    // 写线程函数
    void writer_thread_function(size_t duration_seconds) {
        utils::log_info("Writer thread started");

        const auto& all_keys = data_generator_->get_all_keys();
        size_t block_num = initial_load_end_block_;
        auto start_time = std::chrono::steady_clock::now();
        auto end_time = start_time + std::chrono::seconds(duration_seconds);

        std::random_device rd;
        std::mt19937 gen(rd());

        while (std::chrono::steady_clock::now() < end_time) {
            // 准备一个block的更新数据（1万条kv）
            size_t batch_size = std::min(10000UL, config_.total_keys);
            auto update_indices = data_generator_->generate_hotspot_update_indices(batch_size);
            auto random_values = data_generator_->generate_random_values(update_indices.size());

            std::vector<DataRecord> records;
            records.reserve(update_indices.size());

            for (size_t i = 0; i < update_indices.size(); ++i) {
                size_t idx = update_indices[i];
                if (idx >= all_keys.size()) continue;

                DataRecord record{
                    block_num,
                    all_keys[idx],
                    random_values[i]
                };
                records.push_back(record);
            }

            // 执行写入并测量耗时
            auto write_start = std::chrono::high_resolution_clock::now();
            bool success = db_manager_->write_batch(records);
            auto write_end = std::chrono::high_resolution_clock::now();

            if (!success) {
                utils::log_error("Writer thread: Failed to write batch at block {}", block_num);
                break;
            }

            double write_latency_ms = std::chrono::duration<double, std::milli>(write_end - write_start).count();

            // 记录写入性能
            {
                std::lock_guard<std::mutex> lock(perf_mutex_);
                write_latencies_.push_back(write_latency_ms);
                write_count_++;
            }

            // 更新当前最大block号
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                current_max_block_ = block_num;
            }

            utils::log_info("Writer thread: Completed block {}, write_latency_ms={:.3f}",
                           block_num, write_latency_ms);

            block_num++;

            // 等待3秒
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }

        utils::log_info("Writer thread completed {} blocks", block_num - initial_load_end_block_);
    }

    // 读线程函数
    void reader_thread_function(int thread_id, size_t queries_per_thread) {
        utils::log_info("Reader thread {} started, queries_per_thread={}", thread_id, queries_per_thread);

        const auto& all_keys = data_generator_->get_all_keys();

        std::random_device rd;
        std::mt19937 gen(rd() + thread_id);  // 每个线程使用不同的种子

        size_t successful_queries = 0;
        std::vector<double> thread_query_latencies;
        thread_query_latencies.reserve(queries_per_thread);

        for (size_t i = 0; i < queries_per_thread; ++i) {
            // 获取当前最大block号（用于查询范围）
            BlockNum max_block;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                max_block = current_max_block_;
            }

            if (max_block == 0) {
                // 如果还没有数据，等待一小段时间
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // 随机选择key和target block
            std::uniform_int_distribution<size_t> key_dist(0, all_keys.size() - 1);
            std::uniform_int_distribution<BlockNum> version_dist(0, max_block);

            size_t key_idx = key_dist(gen);
            BlockNum target_version = version_dist(gen);
            const std::string& key = all_keys[key_idx];

            // 执行历史查询
            auto query_start = std::chrono::high_resolution_clock::now();
            auto result = db_manager_->query_historical_version(key, target_version);
            auto query_end = std::chrono::high_resolution_clock::now();

            double latency_ms = std::chrono::duration<double, std::milli>(query_end - query_start).count();
            thread_query_latencies.push_back(latency_ms);

            if (result.has_value()) {
                successful_queries++;
            }

            // 每个线程每50次查询记录一次进度
            if ((i + 1) % 50 == 0) {
                utils::log_info("Reader thread {}: {}/{} queries completed, success_rate={:.1f}%",
                               thread_id, i + 1, queries_per_thread,
                               (successful_queries * 100.0 / (i + 1)));
            }
        }

        // 将线程的查询结果合并到全局统计
        {
            std::lock_guard<std::mutex> lock(perf_mutex_);
            query_latencies_.insert(query_latencies_.end(),
                                  thread_query_latencies.begin(),
                                  thread_query_latencies.end());
            total_successful_queries_ += successful_queries;
        }

        utils::log_info("Reader thread {} completed: {}/{} queries successful ({:.1f}%)",
                       thread_id, successful_queries, queries_per_thread,
                       (successful_queries * 100.0 / queries_per_thread));
    }

    // 打印性能统计
    void print_performance_statistics(size_t test_duration_seconds) {
        std::lock_guard<std::mutex> lock(perf_mutex_);

        utils::log_info("=== Concurrent Read-Write Performance Statistics ===");
        utils::log_info("Test duration: {} seconds", test_duration_seconds);
        utils::log_info("Write operations: {}", write_count_.load());
        utils::log_info("Query operations: {}", query_latencies_.size());
        utils::log_info("Successful queries: {}", total_successful_queries_.load());

        if (!query_latencies_.empty()) {
            std::vector<double> sorted_latencies = query_latencies_;
            std::sort(sorted_latencies.begin(), sorted_latencies.end());

            double sum = std::accumulate(query_latencies_.begin(), query_latencies_.end(), 0.0);
            double avg = sum / query_latencies_.size();

            double p50 = sorted_latencies[sorted_latencies.size() * 0.5];
            double p95 = sorted_latencies[sorted_latencies.size() * 0.95];
            double p99 = sorted_latencies[sorted_latencies.size() * 0.99];
            double max_latency = sorted_latencies.back();
            double min_latency = sorted_latencies.front();

            utils::log_info("=== Query Performance ===");
            utils::log_info("Count: {}", query_latencies_.size());
            utils::log_info("Average: {:.3f} ms", avg);
            utils::log_info("Min: {:.3f} ms", min_latency);
            utils::log_info("Max: {:.3f} ms", max_latency);
            utils::log_info("P50: {:.3f} ms", p50);
            utils::log_info("P95: {:.3f} ms", p95);
            utils::log_info("P99: {:.3f} ms", p99);
            utils::log_info("Query OPS: {:.2f}", static_cast<double>(query_latencies_.size()) / test_duration_seconds);
            utils::log_info("Success Rate: {:.2f}%",
                           (total_successful_queries_.load() * 100.0 / query_latencies_.size()));
        }

        if (!write_latencies_.empty()) {
            std::vector<double> sorted_write_latencies = write_latencies_;
            std::sort(sorted_write_latencies.begin(), sorted_write_latencies.end());

            double sum = std::accumulate(write_latencies_.begin(), write_latencies_.end(), 0.0);
            double avg = sum / write_latencies_.size();

            double p50 = sorted_write_latencies[sorted_write_latencies.size() * 0.5];
            double p95 = sorted_write_latencies[sorted_write_latencies.size() * 0.95];
            double p99 = sorted_write_latencies[sorted_write_latencies.size() * 0.99];

            utils::log_info("=== Write Performance ===");
            utils::log_info("Count: {}", write_latencies_.size());
            utils::log_info("Average: {:.3f} ms", avg);
            utils::log_info("P50: {:.3f} ms", p50);
            utils::log_info("P95: {:.3f} ms", p95);
            utils::log_info("P99: {:.3f} ms", p99);
            utils::log_info("Write OPS: {:.2f}", static_cast<double>(write_count_.load()) / test_duration_seconds);
        }

        utils::log_info("=== End Statistics ===");
    }

    // 执行并发读写测试
    void run_concurrent_test(size_t test_duration_seconds,
                           size_t reader_thread_count,
                           size_t queries_per_thread) {
        utils::log_info("=== Starting Concurrent Read-Write Test ===");
        utils::log_info("Reader threads: {}, Queries per thread: {}",
                       reader_thread_count, queries_per_thread);
        utils::log_info("Test duration: {} seconds", test_duration_seconds);

        // 启动写线程
        std::thread writer_thread(&ConcurrentReadWriteTest::writer_thread_function,
                                 this, test_duration_seconds);

        // 等待一秒让写线程先开始
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // 启动读线程
        std::vector<std::thread> reader_threads;
        reader_threads.reserve(reader_thread_count);

        auto start_time = std::chrono::steady_clock::now();

        for (size_t i = 0; i < reader_thread_count; ++i) {
            reader_threads.emplace_back(&ConcurrentReadWriteTest::reader_thread_function,
                                       this, static_cast<int>(i), queries_per_thread);
        }

        // 等待所有线程完成
        writer_thread.join();

        for (auto& thread : reader_threads) {
            thread.join();
        }

        auto end_time = std::chrono::steady_clock::now();
        size_t actual_duration = std::chrono::duration_cast<std::chrono::seconds>(
            end_time - start_time).count();

        // 打印性能统计
        print_performance_statistics(actual_duration);

        utils::log_info("=== Concurrent Read-Write Test Completed Successfully ===");
    }

private:
    BenchmarkConfig config_;
    std::shared_ptr<StrategyDBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    std::unique_ptr<DataGenerator> data_generator_;
    std::string test_db_path_;

    // 测试状态
    BlockNum initial_load_end_block_ = 0;
    std::atomic<BlockNum> current_max_block_{0};

    // 性能统计（需要锁保护）
    std::mutex perf_mutex_;
    std::vector<double> write_latencies_;
    std::vector<double> query_latencies_;
    std::atomic<size_t> write_count_{0};
    std::atomic<size_t> total_successful_queries_{0};

    // 状态保护
    std::mutex state_mutex_;
};

int main() {
    std::cout << "=== Concurrent Read-Write Test Suite ===" << std::endl;

    try {
        ConcurrentReadWriteTest test;

        // 1. 执行初始加载
        test.run_initial_load();

        // 2. 运行简单并发测试（调试用）
        std::cout << "\n--- Running Simple Concurrent Test (Debug) ---" << std::endl;
        test.run_concurrent_test(
            10,    // 10秒测试
            5,     // 5个读线程
            20     // 每个线程20次查询
        );

        std::cout << "\n--- Running Full Concurrent Test ---" << std::endl;
        test.run_concurrent_test(
            30,    // 30秒测试
            50,    // 50个读线程（先减少到50进行测试）
            200    // 每个线程200次查询
        );

        std::cout << "\n--- All tests completed successfully! ---" << std::endl;
        return 0;

    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}