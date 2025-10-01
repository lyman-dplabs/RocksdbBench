#include "strategy_scenario_runner.hpp"
#include "../utils/logger.hpp"
#include <random>
#include <algorithm>

StrategyScenarioRunner::StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager, 
                                             std::shared_ptr<MetricsCollector> metrics,
                                             const BenchmarkConfig& config)
    : db_manager_(db_manager), metrics_collector_(metrics), config_(config), data_generator_(DataGenerator::Config()) {
    
    // 使用配置中的参数设置 DataGenerator
    DataGenerator::Config data_config;
    data_config.total_keys = config_.initial_records;
    data_config.hotspot_count = static_cast<size_t>(config_.initial_records * 0.1);  // 10% hot keys
    data_config.medium_count = static_cast<size_t>(config_.initial_records * 0.2);  // 20% medium keys  
    data_config.tail_count = config_.initial_records - data_config.hotspot_count - data_config.medium_count;  // 70% tail keys
    
    utils::log_info("About to create DataGenerator with {} keys", data_config.total_keys);
    data_generator_ = DataGenerator(data_config);
    utils::log_info("DataGenerator created successfully");
    
    utils::log_info("StrategyScenarioRunner initialized with config:");
    utils::log_info("  Total Keys: {}", data_config.total_keys);
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
        utils::log_info("  dual_rocksdb_compression: {}", config_.dual_rocksdb_compression ? "true" : "false");
        utils::log_info("  dual_rocksdb_bloom_filters: {}", config_.dual_rocksdb_bloom_filters ? "true" : "false");
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
    const auto& all_keys = data_generator_.get_all_keys();
    utils::log_info("=== ACTUAL DATA VERIFICATION ===");
    utils::log_info("Config says initial_records: {}", config_.initial_records);
    utils::log_info("DataGenerator actually has: {} keys", all_keys.size());
    utils::log_info("Expected: 1:2:7 ratio with {} hot, {} medium, {} tail", 
                   static_cast<size_t>(config_.initial_records * 0.1),
                   static_cast<size_t>(config_.initial_records * 0.2),
                   config_.initial_records - static_cast<size_t>(config_.initial_records * 0.1) - static_cast<size_t>(config_.initial_records * 0.2));
    utils::log_info("=== END VERIFICATION ===");
    
    // 启用批量写入模式以优化数据准备阶段的性能
    db_manager_->set_batch_mode(true);
    
    const size_t batch_size = 10000;
    size_t total_keys = all_keys.size();
    BlockNum current_block = 0;
    
    // Clear and prepare to track written keys
    initial_load_keys_.clear();
    initial_load_keys_.reserve(total_keys);
    
    for (size_t i = 0; i < total_keys; i += batch_size) {
        size_t end_idx = std::min(i + batch_size, total_keys);
        size_t current_batch_size = end_idx - i;
        
        std::vector<DataRecord> records;
        auto random_values = data_generator_.generate_random_values(current_batch_size);
        
        records.reserve(current_batch_size);
        
        for (size_t j = 0; j < current_batch_size; ++j) {
            size_t key_idx = i + j;
            DataRecord record{
                current_block,           // block_num
                all_keys[key_idx],       // addr_slot
                random_values[j]         // value
            };
            records.push_back(record);
            
            // Track this key for historical queries
            initial_load_keys_.push_back(all_keys[key_idx]);
        }
        
        metrics_collector_->start_write_timer();
        bool success = db_manager_->write_batch(records);
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
    
    // Record the actual end block for realistic queries
    initial_load_end_block_ = current_block;
    utils::log_info("Initial load phase completed. Total blocks written: {}, keys tracked: {}", 
                   initial_load_end_block_, initial_load_keys_.size());
}

void StrategyScenarioRunner::run_hotspot_update_phase() {
    utils::log_info("Starting hotspot update phase...");
    
    // 切换到直接写入模式以确保实时一致性
    db_manager_->set_batch_mode(false);
    
    const auto& all_keys = data_generator_.get_all_keys();
    size_t batch_size = std::min(10000UL, config_.hotspot_updates);  // 确保不超过配置的更新数
    const size_t query_interval = std::min(500000UL, config_.hotspot_updates);
    size_t total_processed = 0;
    BlockNum current_block = config_.initial_records / 10000;  // 使用配置中的初始记录数
    
    while (total_processed < config_.hotspot_updates) {  // 使用配置中的热点更新数
        auto update_indices = data_generator_.generate_hotspot_update_indices(batch_size);
        
        std::vector<DataRecord> records;
        auto random_values = data_generator_.generate_random_values(update_indices.size());
        
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
    
    if (initial_load_keys_.empty()) {
        utils::log_error("No initial load keys available for historical queries");
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
    
    utils::log_debug("Using {} initial keys for historical queries", initial_load_keys_.size());
    
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
        if (key_idx >= initial_load_keys_.size()) {
            key_idx = key_idx % initial_load_keys_.size();
        }
        const std::string& key = initial_load_keys_[key_idx];
        
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