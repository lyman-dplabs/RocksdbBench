#include "strategy_scenario_runner.hpp"
#include "../utils/logger.hpp"
#include <random>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <numeric>

StrategyScenarioRunner::StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager, 
                                             std::shared_ptr<MetricsCollector> metrics,
                                             const BenchmarkConfig& config)
    : db_manager_(db_manager), metrics_collector_(metrics), config_(config) {
    
    // 使用配置中的参数设置 DataGenerator
    DataGenerator::Config data_config;
    data_config.total_keys = config_.initial_records;
    data_config.hotspot_count = static_cast<size_t>(config_.initial_records * 0.1);  // 10% hot keys
    data_config.medium_count = static_cast<size_t>(config_.initial_records * 0.2);  // 20% medium keys  
    data_config.tail_count = config_.initial_records - data_config.hotspot_count - data_config.medium_count;  // 70% tail keys
    
    utils::log_info("About to create DataGenerator with {} keys", data_config.total_keys);
    
    data_generator_ = std::make_unique<DataGenerator>(data_config);
    
    utils::log_info("DataGenerator created successfully");
    
    const auto& all_keys = data_generator_->get_all_keys();
    utils::log_info("StrategyScenarioRunner initialized with config:");
    utils::log_info("  Total Keys: {}", all_keys.size());
    utils::log_info("  Initial Records: {}", config_.initial_records);
    utils::log_info("  Hotspot Updates: {}", config_.hotspot_updates);
    utils::log_info("  Hot/Medium/Tail Keys: {} / {} / {}", 
                   data_config.hotspot_count, data_config.medium_count, data_config.tail_count);
    
    // 验证DualRocksDB配置是否被正确接收
    if (config_.storage_strategy == "dual_rocksdb_adaptive") {
        utils::log_info("=== DUALROCKSDB CONFIG VERIFICATION ===");
        utils::log_info("  dual_rocksdb_range_size: {}", config_.dual_rocksdb_range_size);
        utils::log_info("  dual_rocksdb_cache_size: {} MB", config_.dual_rocksdb_cache_size / (1024 * 1024));
        utils::log_info("  dual_rocksdb_hot_ratio: {:.3f}", config_.dual_rocksdb_hot_ratio);
        utils::log_info("  dual_rocksdb_medium_ratio: {:.3f}", config_.dual_rocksdb_medium_ratio);
        utils::log_info("  enable_compression (global): {}", config_.enable_compression ? "true" : "false");
        utils::log_info("  bloom_filters: always enabled");
        utils::log_info("  dual_rocksdb_batch_size: {}", config_.dual_rocksdb_batch_size);
        utils::log_info("  dual_rocksdb_max_batch_bytes: {} MB", config_.dual_rocksdb_max_batch_bytes / (1024 * 1024));
        utils::log_info("=== END DUALROCKSDB CONFIG VERIFICATION ===");
    }
    
    // Set merge callback for metrics collection (for strategies that support it)
    db_manager_->set_merge_callback([this](size_t merged_values, size_t merged_value_size) {
        metrics_collector_->record_merge_operation(merged_values, merged_value_size);
    });
}

void StrategyScenarioRunner::run_initial_load_phase() {
    utils::log_info("Starting initial load phase...");
    
    // DEBUG: 验证实际的数据大小
    const auto& all_keys = data_generator_->get_all_keys();
    utils::log_info("=== ACTUAL DATA VERIFICATION ===");
    utils::log_info("Config says initial_records: {}", config_.initial_records);
    utils::log_info("DataGenerator actually has: {} keys", all_keys.size());
    utils::log_info("Expected: 1:2:7 ratio with {} hot, {} medium, {} tail", 
                   static_cast<size_t>(config_.initial_records * 0.1),
                   static_cast<size_t>(config_.initial_records * 0.2),
                   config_.initial_records - static_cast<size_t>(config_.initial_records * 0.1) - static_cast<size_t>(config_.initial_records * 0.2));
    utils::log_info("=== END VERIFICATION ===");
    
    const size_t batch_size = 10000;
    size_t total_keys = all_keys.size();
    BlockNum current_block = 0;
    
    // 这里整个似乎都可以并行化
    for (size_t i = 0; i < total_keys; i += batch_size) {
        size_t end_idx = std::min(i + batch_size, total_keys);
        size_t current_batch_size = end_idx - i;
        
        std::vector<DataRecord> records;
        auto random_values = data_generator_->generate_random_values(current_batch_size);
        
        records.reserve(current_batch_size);
        
        // TODO: parallem?
        for (size_t j = 0; j < current_batch_size; ++j) {
            size_t key_idx = i + j;
            DataRecord record{
                current_block,           // block_num
                all_keys[key_idx],       // addr_slot
                random_values[j]         // value
            };
            records.push_back(record);
        }
        
        metrics_collector_->start_write_timer();
        bool success = db_manager_->write_initial_load_batch(records);
        metrics_collector_->stop_and_record_write(records.size(), 
                                                 records.size() * (32 + all_keys[0].size()));
        
        if (!success) {
            utils::log_error("Failed to write batch at block {}", current_block);
            break;
        }
        
        current_block++;
        
        if (i % 100000 == 0) {
            utils::log_info("Initial load progress: {}/{} ({:.1f}%)", 
                           i, total_keys, (i * 100.0 / total_keys));
        }
    }

    db_manager_->flush_all_batches();
    
    // Record the actual end block for realistic queries
    initial_load_end_block_ = current_block;
    current_max_block_ = current_block - 1; // Update current max block
    utils::log_info("Initial load phase completed. Total blocks written: {}, keys tracked: {}", 
                   initial_load_end_block_, total_keys);
}

void StrategyScenarioRunner::run_hotspot_update_phase() {
    utils::log_info("Starting hotspot update phase...");
    
    const auto& all_keys = data_generator_->get_all_keys();
    size_t batch_size = std::min(10000UL, config_.hotspot_updates);  // 确保不超过配置的更新数
    const size_t query_interval = std::min(500000UL, config_.hotspot_updates);
    size_t total_processed = 0;
    BlockNum current_block = config_.initial_records / 10000;  // 使用配置中的初始记录数
    
    while (total_processed < config_.hotspot_updates) {  // 使用配置中的热点更新数
        auto update_indices = data_generator_->generate_hotspot_update_indices(batch_size);
        
        std::vector<DataRecord> records;
        auto random_values = data_generator_->generate_random_values(update_indices.size());
        
        records.reserve(update_indices.size());
        
        for (size_t i = 0; i < update_indices.size(); ++i) {
            size_t idx = update_indices[i];
            if (idx >= all_keys.size()) continue;
            
            DataRecord record{
                current_block,       // block_num
                all_keys[idx],       // addr_slot
                random_values[i]     // value
            };
            records.push_back(record);
        }
        
        metrics_collector_->start_write_timer();
        bool success = db_manager_->write_batch(records);
        metrics_collector_->stop_and_record_write(records.size(), 
                                                 records.size() * (32 + all_keys[0].size()));
        
        if (!success) {
            utils::log_error("Failed to write update batch at block {}", current_block);
            break;
        }
        
        total_processed += records.size();
        current_block++;
        
        if (total_processed % query_interval == 0) {
            run_historical_queries(100);
        }
        
        if (total_processed % 100000 == 0) {
            utils::log_info("Hotspot update progress: {}/{}", total_processed, config_.hotspot_updates);
        }
    }
    
    // Record the actual end block for realistic queries
    hotspot_update_end_block_ = current_block;
    utils::log_info("Hotspot update phase completed. Total processed: {}, final block: {}", total_processed, hotspot_update_end_block_);
}

void StrategyScenarioRunner::run_historical_queries(size_t query_count) {
    utils::log_info("Running {} historical queries...", query_count);
    
    // Get all keys from data generator
    const auto& all_keys = data_generator_->get_all_keys();
    if (all_keys.empty()) {
        utils::log_error("No keys available for historical queries");
        return;
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // Define key type ranges based on 1:2:7 ratio (hot:medium:tail)
    const size_t hot_key_count = 10000000;      // 10M hot keys (0 to 9,999,999)
    const size_t medium_key_count = 20000000;   // 20M medium keys (10M to 29,999,999)
    const size_t tail_key_count = 70000000;     // 70M tail keys (30M to 99,999,999)
    
    // Weighted distribution: 1:2:7 (hot:medium:tail)
    std::discrete_distribution<int> type_dist({1, 2, 7}); // hot, medium, tail
    std::uniform_int_distribution<size_t> hot_dist(0, hot_key_count - 1);
    std::uniform_int_distribution<size_t> medium_dist(hot_key_count, hot_key_count + medium_key_count - 1);
    std::uniform_int_distribution<size_t> tail_dist(hot_key_count + medium_key_count, 
                                                   hot_key_count + medium_key_count + tail_key_count - 1);
    
    utils::log_debug("Using {} initial keys for historical queries", all_keys.size());
    
    for (size_t i = 0; i < query_count; ++i) {
        // Select key type based on 1:2:7 ratio
        int key_type = type_dist(gen);
        size_t key_idx;
        
        switch (key_type) {
            case 0: // hot keys (0 to 9,999,999)
                key_idx = hot_dist(gen);
                break;
            case 1: // medium keys (10M to 29,999,999)
                key_idx = medium_dist(gen);
                break;
            case 2: // tail keys (30M to 99,999,999)
                key_idx = tail_dist(gen);
                break;
            default:
                key_idx = hot_dist(gen); // fallback
        }
        
        // Get the actual key (ensure it's within bounds)
        if (key_idx >= all_keys.size()) {
            key_idx = key_idx % all_keys.size();
        }
        const std::string& key = all_keys[key_idx];
        
        // For historical queries, we query the latest value for the key
        // This simulates "what is the current state of this key"
        metrics_collector_->start_query_timer();
        auto result = db_manager_->query_latest_value(key);
        metrics_collector_->stop_and_record_query(result.has_value());
        
        // Determine key type for cache hit analysis based on selection
        std::string key_type_str;
        switch (key_type) {
            case 0: // hot keys
                key_type_str = "hot";
                break;
            case 1: // medium keys
                key_type_str = "medium";
                break;
            case 2: // tail keys
                key_type_str = "tail";
                break;
            default:
                key_type_str = "hot"; // fallback
        }
        
        // Record cache hit metrics (simulate cache behavior)
        bool cache_hit = result.has_value() && (gen() % 100 < 80);  // 80% cache hit rate
        metrics_collector_->record_cache_hit(key_type_str, cache_hit);
    }
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

// ===== 新的实现：连续更新查询循环 =====

void StrategyScenarioRunner::run_continuous_update_query_loop(size_t duration_minutes) {
    utils::log_info("Starting continuous update-query loop for {} minutes...", duration_minutes);
    
    perf_metrics_.start_time = std::chrono::steady_clock::now();
    auto end_time = perf_metrics_.start_time + std::chrono::minutes(duration_minutes);
    
    size_t block_num = config_.initial_records / 10000; // 从初始加载结束后的block开始
    size_t batch_size = std::min(10000UL, config_.hotspot_updates);
    size_t update_count = 0;
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    while (std::chrono::steady_clock::now() < end_time) {
        // 每次处理一个block（batch）
        run_single_block_update_query(block_num);
        
        block_num++;
        update_count++;
        current_max_block_ = block_num;
        
        // 每1000次更新输出进度（减少日志频率）
        if (update_count % 1000 == 0) {
            auto elapsed = std::chrono::duration_cast<std::chrono::minutes>(
                std::chrono::steady_clock::now() - perf_metrics_.start_time).count();
            utils::log_info("Progress: {} updates completed, {} minutes elapsed", 
                           update_count, elapsed);
        }
    }
    
    perf_metrics_.end_time = std::chrono::steady_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        perf_metrics_.end_time - perf_metrics_.start_time).count();
    
    utils::log_info("Continuous update-query loop completed. Total updates: {}, Duration: {} seconds", 
                   update_count, total_duration);
    
    // 打印性能统计
    perf_metrics_.print_statistics();
}

void StrategyScenarioRunner::run_single_block_update_query(size_t block_num) {
    const auto& all_keys = data_generator_->get_all_keys();
    if (all_keys.empty()) {
        utils::log_error("No keys available for update-query operations");
        return;
    }
    
    // 1. 准备更新数据
    size_t batch_size = std::min(10000UL, config_.hotspot_updates);
    auto update_indices = data_generator_->generate_hotspot_update_indices(batch_size);
    auto random_values = data_generator_->generate_random_values(update_indices.size());
    
    std::vector<DataRecord> records;
    records.reserve(update_indices.size());
    
    for (size_t i = 0; i < update_indices.size(); ++i) {
        size_t idx = update_indices[i];
        if (idx >= all_keys.size()) continue;
        
        DataRecord record{
            block_num,              // block_num
            all_keys[idx],          // addr_slot
            random_values[i]        // value
        };
        records.push_back(record);
    }
    
    // 2. 执行更新并测量耗时
    auto write_start = std::chrono::high_resolution_clock::now();
    bool success = db_manager_->write_batch(records);
    auto write_end = std::chrono::high_resolution_clock::now();
    
    double write_latency_ms = std::chrono::duration<double, std::milli>(write_end - write_start).count();
    perf_metrics_.write_latencies_ms.push_back(write_latency_ms);
    
    if (!success) {
        utils::log_error("Failed to write batch at block {}", block_num);
        return;
    }
    
    // 3. 立即执行历史版本查询（记录整个block的查询时延）
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> key_dist(0, all_keys.size() - 1);
    std::uniform_int_distribution<BlockNum> version_dist(0, current_max_block_);
    
    // 每次更新后进行多个随机历史查询
    const size_t queries_per_update = 5;
    size_t successful_queries = 0;
    
    // 记录整个block查询的开始时间
    auto block_query_start = std::chrono::high_resolution_clock::now();
    
    for (size_t i = 0; i < queries_per_update; ++i) {
        size_t key_idx = key_dist(gen);
        BlockNum target_version = version_dist(gen);
        const std::string& key = all_keys[key_idx];
        
        auto query_result = query_historical_version(key, target_version);
        
        if (query_result.found) {
            successful_queries++;
        }
        
        // 只记录失败的查询（减少日志量）
        if (!query_result.found) {
            utils::log_debug("QUERY_FAILED: key={}, target_version={}, latency_ms={:.3f}",
                           key.substr(0, 8), target_version, query_result.latency_ms);
        }
    }
    
    // 记录整个block的查询时延
    auto block_query_end = std::chrono::high_resolution_clock::now();
    double block_query_latency_ms = std::chrono::duration<double, std::milli>(block_query_end - block_query_start).count();
    
    // 记录block级别的查询统计
    utils::log_info("BLOCK_QUERY_STATS: queries={}, successful={}, block_latency_ms={:.3f}",
                   queries_per_update, successful_queries, block_query_latency_ms);
    
    // 将block级别的查询时延添加到性能统计中
    perf_metrics_.query_latencies_ms.push_back(block_query_latency_ms);
    
    // 4. 记录性能指标（便于grep/awk处理）
    log_performance_metrics("WRITE_BATCH", write_latency_ms);
}

StrategyScenarioRunner::QueryResult StrategyScenarioRunner::query_historical_version(const std::string& addr_slot, BlockNum target_version) {
    auto query_start = std::chrono::high_resolution_clock::now();
    
    // 调用底层strategy的查询接口
    auto result = db_manager_->query_historical_version(addr_slot, target_version);
    
    auto query_end = std::chrono::high_resolution_clock::now();
    double latency_ms = std::chrono::duration<double, std::milli>(query_end - query_start).count();
    
    perf_metrics_.query_latencies_ms.push_back(latency_ms);
    
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

void StrategyScenarioRunner::log_performance_metrics(const std::string& operation_type, double latency_ms) {
    // 只记录每次写入性能，但可以考虑进一步减少频率
    static size_t write_counter = 0;
    write_counter++;
    
    // 每100次写入记录一次性能指标（减少日志量）
    if (write_counter % 100 == 0) {
        auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - perf_metrics_.start_time).count();
        
        utils::log_info("PERF_METRIC: timestamp_ms={}, operation={}, latency_ms={:.6f}", 
                       timestamp, operation_type, latency_ms);
    }
}

// ===== 性能统计实现 =====

void StrategyScenarioRunner::PerformanceMetrics::print_statistics() const {
    if (write_latencies_ms.empty() && query_latencies_ms.empty()) {
        utils::log_info("No performance data to report");
        return;
    }
    
    auto calculate_stats = [](const std::vector<double>& latencies, const std::string& type) {
        if (latencies.empty()) return;
        
        std::vector<double> sorted_latencies = latencies;
        std::sort(sorted_latencies.begin(), sorted_latencies.end());
        
        double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
        double avg = sum / latencies.size();
        
        double p50 = sorted_latencies[sorted_latencies.size() * 0.5];
        double p95 = sorted_latencies[sorted_latencies.size() * 0.95];
        double p99 = sorted_latencies[sorted_latencies.size() * 0.99];
        double max = sorted_latencies.back();
        double min = sorted_latencies.front();
        
        utils::log_info("=== {} Performance Statistics ===", type);
        utils::log_info("Count: {}", latencies.size());
        utils::log_info("Average: {:.3f} ms", avg);
        utils::log_info("Min: {:.3f} ms", min);
        utils::log_info("Max: {:.3f} ms", max);
        utils::log_info("P50: {:.3f} ms", p50);
        utils::log_info("P95: {:.3f} ms", p95);
        utils::log_info("P99: {:.3f} ms", p99);
    };
    
    calculate_stats(write_latencies_ms, "WRITE_BATCH");
    calculate_stats(query_latencies_ms, "HISTORICAL_QUERY");
    
    auto total_duration = std::chrono::duration_cast<std::chrono::seconds>(
        end_time - start_time).count();
    utils::log_info("Total test duration: {} seconds", total_duration);
    utils::log_info("Write ops per second: {:.2f}", 
                   static_cast<double>(write_latencies_ms.size()) / total_duration);
    utils::log_info("Query ops per second: {:.2f}", 
                   static_cast<double>(query_latencies_ms.size()) / total_duration);
}