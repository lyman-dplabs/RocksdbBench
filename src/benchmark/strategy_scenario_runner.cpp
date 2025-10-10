#include "strategy_scenario_runner.hpp"
#include "../utils/logger.hpp"
#include <random>
#include <algorithm>
#include <chrono>
#include <numeric>

using namespace utils;

thread_local std::vector<double> StrategyScenarioRunner::thread_query_latencies_;


StrategyScenarioRunner::StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager,
                                             std::shared_ptr<MetricsCollector> metrics,
                                             const BenchmarkConfig& config)
    : db_manager_(db_manager), metrics_collector_(metrics), config_(config) {

    DataGenerator::Config data_config;
    data_config.total_keys = config_.total_keys;
    data_config.hotspot_count = static_cast<size_t>(config_.total_keys * 0.1);  // 10% hot keys
    data_config.medium_count = static_cast<size_t>(config_.total_keys * 0.2);  // 20% medium keys
    data_config.tail_count = config_.total_keys - data_config.hotspot_count - data_config.medium_count;  // 70% tail keys

    utils::log_info("About to create DataGenerator with {} keys", data_config.total_keys);

    data_generator_ = std::make_unique<DataGenerator>(data_config);

    utils::log_info("DataGenerator created successfully");

    const auto& all_keys = data_generator_->get_all_keys();
    utils::log_info("StrategyScenarioRunner initialized with config:");
    utils::log_info("  Total Keys: {}", all_keys.size());
    utils::log_info("  Test Duration: {} minutes", config_.continuous_duration_minutes);
    utils::log_info("  Hot/Medium/Tail Keys: {} / {} / {}",
                   data_config.hotspot_count, data_config.medium_count, data_config.tail_count);

    // Set merge callback for metrics collection (for strategies that support it)
    db_manager_->set_merge_callback([this](size_t merged_values, size_t merged_value_size) {
        metrics_collector_->record_merge_operation(merged_values, merged_value_size);
    });
}

// 新的构造函数：支持外部DataGenerator（用于recovery test）
StrategyScenarioRunner::StrategyScenarioRunner(
    std::shared_ptr<StrategyDBManager> db_manager,
    std::shared_ptr<MetricsCollector> metrics, const BenchmarkConfig &config,
    std::unique_ptr<DataGenerator> external_data_generator,
    size_t initial_load_end_block,
    size_t max_block)
    : db_manager_(db_manager), metrics_collector_(metrics), config_(config),
      data_generator_(std::move(external_data_generator)),
      initial_load_end_block_(initial_load_end_block),
      current_max_block_(max_block) {

    utils::log_info("StrategyScenarioRunner initialized with external DataGenerator");

    const auto& all_keys = data_generator_->get_all_keys();
    utils::log_info("StrategyScenarioRunner initialized with config:");
    utils::log_info("  Total Keys: {}", all_keys.size());
    utils::log_info("  Test Duration: {} minutes", config_.continuous_duration_minutes);
    utils::log_info("  Using external recovered keys for testing");

    // Set merge callback for metrics collection (for strategies that support it)
    db_manager_->set_merge_callback([this](size_t merged_values, size_t merged_value_size) {
        metrics_collector_->record_merge_operation(merged_values, merged_value_size);
    });
}

void StrategyScenarioRunner::run_initial_load_phase() {
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

// ===== 新的并发读写测试实现 =====

void StrategyScenarioRunner::run_concurrent_read_write_test(const ConcurrentTestConfig& test_config) {
    unsigned int cpu_cores = std::thread::hardware_concurrency();
    utils::log_info("=== Starting Concurrent Read-Write Test (Optimized Lock Design) ===");
    utils::log_info("Hardware: {} CPU cores detected", cpu_cores);
    utils::log_info("Reader threads: {} (CPU cores x 2), Continuous queries during test",
                   test_config.reader_thread_count);
    utils::log_info("Test duration: {} seconds", test_config.test_duration_seconds);
    utils::log_info("Write sleep: {} seconds, Block size: {} kv",
                   test_config.write_sleep_seconds, test_config.block_size);
    utils::log_info("=== Lock Design: Write/Read separated + Thread-local storage ===");

    // 清空之前的统计数据（分离锁操作）
    {
        utils::log_debug("CLEAR_WRITE_LOCK: Acquiring write_perf_mutex_ to clear write stats");
        std::lock_guard<std::mutex> lock(write_perf_mutex_);
        write_latencies_.clear();
        write_count_ = 0;
        utils::log_debug("CLEAR_WRITE_LOCK: Released write_perf_mutex_");
    }

    {
        utils::log_debug("CLEAR_QUERY_LOCK: Acquiring query_merge_mutex_ to clear query stats");
        std::lock_guard<std::mutex> lock(query_merge_mutex_);
        query_latencies_.clear();
        total_successful_queries_ = 0;
        utils::log_debug("CLEAR_QUERY_LOCK: Released query_merge_mutex_");
    }

    // 启动写线程
    std::thread writer_thread(&StrategyScenarioRunner::writer_thread_function,
                             this, test_config.test_duration_seconds,
                             test_config.write_sleep_seconds,
                             test_config.block_size);

    // 等待一秒让写线程先开始
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // 启动读线程 - 使用CPU核心数*2的数量
    size_t actual_reader_thread_count = ConcurrentTestConfig::get_recommended_reader_threads();
    utils::log_info("Starting {} reader threads based on CPU cores (recommended count)", actual_reader_thread_count);

    std::vector<std::thread> reader_threads;
    reader_threads.reserve(actual_reader_thread_count);

    auto start_time = std::chrono::steady_clock::now();

    test_running_ = true;

    for (size_t i = 0; i < actual_reader_thread_count; ++i) {
        reader_threads.emplace_back(&StrategyScenarioRunner::reader_thread_function,
                                   this, static_cast<int>(i),
                                   std::chrono::seconds(test_config.test_duration_seconds));
    }

    writer_thread.join();

    test_running_ = false;

    for (auto& thread : reader_threads) {
        thread.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    size_t actual_duration = std::chrono::duration_cast<std::chrono::seconds>(
        end_time - start_time).count();

    utils::log_info("=== Concurrent Read-Write Test Completed ===");
    utils::log_info("Actual test duration: {} seconds", actual_duration);

    PerformanceStats stats = get_performance_stats();
    stats.test_duration_seconds = actual_duration;
    stats.print_statistics();
}

void StrategyScenarioRunner::run_continuous_update_query_loop(size_t duration_minutes) {
    utils::log_info("Converting legacy continuous mode to concurrent read-write test");

    ConcurrentTestConfig test_config = ConcurrentTestConfig::from_benchmark_config(config_);
    test_config.test_duration_seconds = duration_minutes * 60;

    // 使用适中的并发配置
    test_config.reader_thread_count = std::min(500UL, config_.total_keys / 1000);  // 根据数据量调整线程数
    test_config.queries_per_thread = 200;
    test_config.write_sleep_seconds = 3;
    test_config.block_size = 10000;

    run_concurrent_read_write_test(test_config);
}

// 写线程函数
void StrategyScenarioRunner::writer_thread_function(size_t duration_seconds,
                                                   size_t sleep_seconds,
                                                   size_t block_size) {
    utils::log_info("Writer thread started");

    const auto& all_keys = data_generator_->get_all_keys();
    size_t block_num = initial_load_end_block_;
    auto start_time = std::chrono::steady_clock::now();
    auto end_time = start_time + std::chrono::seconds(duration_seconds);

    std::random_device rd;
    std::mt19937 gen(rd());

    while (std::chrono::steady_clock::now() < end_time) {
        // 准备一个block的更新数据
        size_t actual_batch_size = std::min(block_size, config_.total_keys);
        auto update_indices = data_generator_->generate_hotspot_update_indices(actual_batch_size);
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

        // 记录写入性能（使用专用写锁）
        {
            utils::log_debug("WRITE_LOCK: Acquiring write_perf_mutex_ for block {}", block_num);
            std::lock_guard<std::mutex> lock(write_perf_mutex_);
            write_latencies_.push_back(write_latency_ms);
            write_count_++;
            utils::log_debug("WRITE_LOCK: Released write_perf_mutex_, total writes: {}", write_count_.load());
        }

        // 更新当前最大block号
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            current_max_block_ = block_num;
        }

        utils::log_info("Writer thread: Completed block {}, write_latency_ms={:.3f}",
                       block_num, write_latency_ms);

        block_num++;

        // 等待指定时间
        std::this_thread::sleep_for(std::chrono::seconds(sleep_seconds));
    }

    utils::log_info("Writer thread completed {} blocks", block_num - initial_load_end_block_);
}

// 读线程函数
void StrategyScenarioRunner::reader_thread_function(int thread_id, std::chrono::seconds test_duration) {
    utils::log_info("Reader thread {} started, duration={} seconds", thread_id, test_duration.count());

    const auto& all_keys = data_generator_->get_all_keys();

    std::random_device rd;
    std::mt19937 gen(rd() + thread_id);  // 每个线程使用不同的种子

    size_t successful_queries = 0;
    size_t total_queries = 0;
    auto start_time = std::chrono::steady_clock::now();

    // 清空线程本地存储（无锁操作）
    thread_query_latencies_.clear();
    thread_query_latencies_.reserve(10000);  // 预分配较大空间

    utils::log_debug("READ_THREAD {}: Using thread-local storage, no lock needed for latencies", thread_id);

    // 在测试持续时间内持续执行查询
    while (test_running_) {
        // 检查是否超过测试持续时间
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed >= test_duration) {
            break;
        }
        BlockNum max_block = current_max_block_;

        std::uniform_int_distribution<size_t> key_dist(0, all_keys.size() - 1);
        std::uniform_int_distribution<BlockNum> version_dist(initial_load_end_block_, max_block);

        size_t key_idx = key_dist(gen);
        BlockNum target_version = version_dist(gen);
        const std::string& key = all_keys[key_idx];

        auto query_result = query_historical_version(key, target_version);

        double latency_ms = query_result.latency_ms;
        thread_query_latencies_.push_back(latency_ms);
        total_queries++;

        if (query_result.found) {
            successful_queries++;
        }

        if (total_queries % 50 == 0) {
            utils::log_info("Reader thread {}: {}/{} queries completed, success_rate={:.1f}%, local_latencies={}",
                           thread_id, total_queries, thread_query_latencies_.size(),
                           (successful_queries * 100.0 / total_queries), thread_query_latencies_.size());
        }
    }

    // 在线程结束时合并到全局统计（只加锁一次）
    {
        utils::log_debug("MERGE_LOCK: Reader thread {} acquiring query_merge_mutex_ to merge {} latencies",
                       thread_id, thread_query_latencies_.size());
        std::lock_guard<std::mutex> lock(query_merge_mutex_);
        query_latencies_.insert(query_latencies_.end(),
                              thread_query_latencies_.begin(),
                              thread_query_latencies_.end());
        total_successful_queries_ += successful_queries;
        utils::log_debug("MERGE_LOCK: Reader thread {} released query_merge_mutex_, total query latencies: {}",
                       thread_id, query_latencies_.size());
    }

    utils::log_info("Reader thread {} completed: {}/{} queries successful ({:.1f}%)",
                   thread_id, successful_queries, total_queries,
                   total_queries > 0 ? (successful_queries * 100.0 / total_queries) : 0.0);
}

StrategyScenarioRunner::QueryResult StrategyScenarioRunner::query_historical_version(const std::string& addr_slot, BlockNum target_version) {
    auto query_start = std::chrono::high_resolution_clock::now();

    auto result = db_manager_->query_historical_version(addr_slot, target_version);

    auto query_end = std::chrono::high_resolution_clock::now();
    double latency_ms = std::chrono::duration<double, std::milli>(query_end - query_start).count();

    QueryResult query_result;
    query_result.found = result.has_value();
    query_result.latency_ms = latency_ms;

    if (result.has_value()) {
        // 解析返回的结果（假设格式为 "block_num:value"）
        auto colon_pos = result->find(':');
        if (colon_pos != std::string::npos) {
            query_result.block_num = std::stoull(result->substr(0, colon_pos));
            query_result.value = result->substr(colon_pos + 1);
        } else {
            query_result.block_num = target_version;
            query_result.value = *result;
        }
    }

    return query_result;
}

// 获取性能统计结果（需要分别获取读写数据）
StrategyScenarioRunner::PerformanceStats StrategyScenarioRunner::get_performance_stats() const {
    PerformanceStats stats;

    utils::log_debug("GET_STATS: Acquiring write_perf_mutex_ to get write stats");
    std::lock_guard<std::mutex> write_lock(write_perf_mutex_);
    stats.total_write_ops = write_count_.load();
    stats.write_latencies_ms = write_latencies_;
    utils::log_debug("GET_STATS: Released write_perf_mutex_, write_ops: {}", stats.total_write_ops);

    utils::log_debug("GET_STATS: Acquiring query_merge_mutex_ to get query stats");
    std::lock_guard<std::mutex> query_lock(query_merge_mutex_);
    stats.total_query_ops = query_latencies_.size();
    stats.successful_queries = total_successful_queries_.load();
    stats.query_latencies_ms = query_latencies_;
    utils::log_debug("GET_STATS: Released query_merge_mutex_, query_ops: {}", stats.total_query_ops);

    calculate_performance_statistics(stats);

    utils::log_debug("GET_STATS: Performance statistics calculated successfully");
    return stats;
}

// 计算性能统计数据
void StrategyScenarioRunner::calculate_performance_statistics(PerformanceStats& stats) const {
    // 计算查询性能统计
    if (!stats.query_latencies_ms.empty()) {
        std::vector<double> sorted_latencies = stats.query_latencies_ms;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());

        double sum = std::accumulate(sorted_latencies.begin(), sorted_latencies.end(), 0.0);
        stats.query_avg_ms = sum / sorted_latencies.size();

        size_t size = sorted_latencies.size();
        stats.query_p50_ms = sorted_latencies[size * 0.5];
        stats.query_p95_ms = sorted_latencies[size * 0.95];
        stats.query_p99_ms = sorted_latencies[size * 0.99];
        stats.query_min_ms = sorted_latencies.front();
        stats.query_max_ms = sorted_latencies.back();

        if (stats.test_duration_seconds > 0) {
            stats.query_ops_per_sec = static_cast<double>(stats.total_query_ops) / stats.test_duration_seconds;
        }
        stats.query_success_rate = (stats.successful_queries * 100.0 / stats.total_query_ops);
    }

    // 计算写入性能统计
    if (!stats.write_latencies_ms.empty()) {
        std::vector<double> sorted_write_latencies = stats.write_latencies_ms;
        std::sort(sorted_write_latencies.begin(), sorted_write_latencies.end());

        double sum = std::accumulate(sorted_write_latencies.begin(), sorted_write_latencies.end(), 0.0);
        stats.write_avg_ms = sum / sorted_write_latencies.size();

        size_t size = sorted_write_latencies.size();
        stats.write_p50_ms = sorted_write_latencies[size * 0.5];
        stats.write_p95_ms = sorted_write_latencies[size * 0.95];
        stats.write_p99_ms = sorted_write_latencies[size * 0.99];

        if (stats.test_duration_seconds > 0) {
            stats.write_ops_per_sec = static_cast<double>(stats.total_write_ops) / stats.test_duration_seconds;
        }
    }
}

// 打印性能统计
void StrategyScenarioRunner::PerformanceStats::print_statistics() const {
    utils::log_info("=== Concurrent Read-Write Performance Statistics ===");
    utils::log_info("Test duration: {:.1f} seconds", test_duration_seconds);
    utils::log_info("Write operations: {}", total_write_ops);
    utils::log_info("Query operations: {}", total_query_ops);
    utils::log_info("Successful queries: {}", successful_queries);

    if (!query_latencies_ms.empty()) {
        utils::log_info("=== Query Performance ===");
        utils::log_info("Count: {}", total_query_ops);
        utils::log_info("Average: {:.3f} ms", query_avg_ms);
        utils::log_info("Min: {:.3f} ms", query_min_ms);
        utils::log_info("Max: {:.3f} ms", query_max_ms);
        utils::log_info("P50: {:.3f} ms", query_p50_ms);
        utils::log_info("P95: {:.3f} ms", query_p95_ms);
        utils::log_info("P99: {:.3f} ms", query_p99_ms);
        utils::log_info("Query OPS: {:.2f}", query_ops_per_sec);
        utils::log_info("Success Rate: {:.2f}%", query_success_rate);
    }

    if (!write_latencies_ms.empty()) {
        utils::log_info("=== Write Performance ===");
        utils::log_info("Count: {}", total_write_ops);
        utils::log_info("Average: {:.3f} ms", write_avg_ms);
        utils::log_info("P50: {:.3f} ms", write_p50_ms);
        utils::log_info("P95: {:.3f} ms", write_p95_ms);
        utils::log_info("P99: {:.3f} ms", write_p99_ms);
        utils::log_info("Write OPS: {:.2f}", write_ops_per_sec);
    }

    utils::log_info("=== End Statistics ===");
}

void StrategyScenarioRunner::collect_rocksdb_statistics() {
    // Collect real bloom filter statistics
    auto bloom_stats = db_manager_->get_bloom_filter_stats();

    utils::log_info("Bloom Filter Summary: hits={}, misses={}, total_queries={}",
                   bloom_stats.hits, bloom_stats.misses, bloom_stats.total_queries);

    if (bloom_stats.total_queries > 0) {
        // Record actual bloom filter performance
        for (uint64_t i = 0; i < bloom_stats.hits; ++i) {
            metrics_collector_->record_bloom_filter_query(true);
        }
        for (uint64_t i = 0; i < bloom_stats.misses; ++i) {
            metrics_collector_->record_bloom_filter_query(false);
        }

        double false_positive_rate = bloom_stats.total_queries > 0 ?
            (static_cast<double>(bloom_stats.misses) / bloom_stats.total_queries) * 100.0 : 0.0;
        utils::log_info("Bloom Filter False Positive Rate: {:.2f}%", false_positive_rate);
    }

    // Collect real compaction statistics
    auto compaction_stats = db_manager_->get_compaction_stats();

    utils::log_info("Compaction Summary: bytes_read={}, bytes_written={}, time_micros={}",
                   compaction_stats.bytes_read, compaction_stats.bytes_written, compaction_stats.time_micros);

    if (compaction_stats.bytes_read > 0) {
        // Estimate compaction count and record metrics
        size_t compaction_count = compaction_stats.bytes_read / (10 * 1024 * 1024); // Rough estimate
        for (size_t i = 0; i < compaction_count; ++i) {
            double avg_time = static_cast<double>(compaction_stats.time_micros) / compaction_count / 1000.0;
            metrics_collector_->record_compaction(avg_time, compaction_stats.bytes_read / compaction_count, 2);
        }
    }
}

std::string StrategyScenarioRunner::get_current_strategy() const {
    return db_manager_->get_strategy_name();
}