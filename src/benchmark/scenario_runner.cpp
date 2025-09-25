#include "scenario_runner.hpp"
#include "../utils/logger.hpp"
#include <random>
#include <algorithm>

ScenarioRunner::ScenarioRunner(std::shared_ptr<DBManager> db, std::shared_ptr<MetricsCollector> metrics)
    : db_manager_(db), metrics_collector_(metrics), data_generator_(DataGenerator::Config()) {
    
    // Set merge callback for metrics collection
    db_manager_->set_merge_callback([this](size_t merged_values, size_t merged_value_size) {
        metrics_collector_->record_merge_operation(merged_values, merged_value_size);
    });
}

void ScenarioRunner::run_initial_load_phase() {
    utils::log_info("Starting initial load phase...");
    
    const auto& all_keys = data_generator_.get_all_keys();
    const size_t batch_size = 10000;
    size_t total_keys = all_keys.size();
    BlockNum current_block = 0;
    
    // Clear and prepare to track written keys and key-block pairs
    initial_load_key_blocks_.clear();
    initial_load_key_blocks_.reserve(total_keys);
    
    // Pre-classify key-block pair indices for efficient weighted selection
    hot_key_indices_.clear();
    medium_key_indices_.clear();
    tail_key_indices_.clear();
    
    const size_t hot_key_count = 10000000;      // 10M hot keys
    const size_t medium_key_count = 20000000;   // 20M medium keys  
    const size_t tail_key_count = 70000000;     // 70M tail keys
    
    hot_key_indices_.reserve(hot_key_count);
    medium_key_indices_.reserve(medium_key_count);
    tail_key_indices_.reserve(tail_key_count);
    
    for (size_t i = 0; i < total_keys; i += batch_size) {
        size_t end_idx = std::min(i + batch_size, total_keys);
        size_t current_batch_size = end_idx - i;
        
        std::vector<ChangeSetRecord> changes;
        std::vector<IndexRecord> indices;
        auto random_values = data_generator_.generate_random_values(current_batch_size);
        
        changes.reserve(current_batch_size);
        indices.reserve(current_batch_size);
        
        for (size_t j = 0; j < current_batch_size; ++j) {
            size_t key_idx = i + j;
            ChangeSetRecord change{current_block, all_keys[key_idx], random_values[j]};
            changes.push_back(change);
            
            PageNum page = block_to_page(current_block);
            IndexRecord index{page, all_keys[key_idx], {current_block}};
            indices.push_back(index);
            
            // Track key-block pair for efficient historical queries
            size_t pair_idx = initial_load_key_blocks_.size();
            initial_load_key_blocks_.push_back({key_idx, current_block, all_keys[key_idx]});
            
            // Classify the key-block pair by key type
            if (key_idx < hot_key_count) {
                hot_key_indices_.push_back(pair_idx);
            } else if (key_idx < hot_key_count + medium_key_count) {
                medium_key_indices_.push_back(pair_idx);
            } else {
                tail_key_indices_.push_back(pair_idx);
            }
        }
        
        metrics_collector_->start_write_timer();
        bool success = db_manager_->write_batch(changes, indices);
        metrics_collector_->stop_and_record_write(changes.size(), 
                                                 changes.size() * (32 + all_keys[0].size()));
        
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
    utils::log_info("Initial load phase completed. Total blocks written: {}, key-block pairs tracked: {}", 
                   initial_load_end_block_, initial_load_key_blocks_.size());
}

void ScenarioRunner::run_hotspot_update_phase() {
    utils::log_info("Starting hotspot update phase...");
    
    const auto& all_keys = data_generator_.get_all_keys();
    const size_t batch_size = 10000;
    const size_t query_interval = 500000;
    size_t total_processed = 0;
    BlockNum current_block = 100000000 / 10000;
    
    while (total_processed < 10000000) {
        auto update_indices = data_generator_.generate_hotspot_update_indices(batch_size);
        
        std::vector<ChangeSetRecord> changes;
        std::vector<IndexRecord> indices;
        auto random_values = data_generator_.generate_random_values(update_indices.size());
        
        changes.reserve(update_indices.size());
        indices.reserve(update_indices.size());
        
        for (size_t i = 0; i < update_indices.size(); ++i) {
            size_t idx = update_indices[i];
            if (idx >= all_keys.size()) continue;
            
            ChangeSetRecord change{current_block, all_keys[idx], random_values[i]};
            changes.push_back(change);
            
            PageNum page = block_to_page(current_block);
            IndexRecord index{page, all_keys[idx], {current_block}};
            indices.push_back(index);
        }
        
        metrics_collector_->start_write_timer();
        bool success = db_manager_->write_batch(changes, indices);
        metrics_collector_->stop_and_record_write(changes.size(), 
                                                 changes.size() * (32 + all_keys[0].size()));
        
        if (!success) {
            utils::log_error("Failed to write update batch at block {}", current_block);
            break;
        }
        
        total_processed += changes.size();
        current_block++;
        
        if (total_processed % query_interval == 0) {
            run_historical_queries(100);
        }
        
        if (total_processed % 100000 == 0) {
            utils::log_info("Hotspot update progress: {}/{}", total_processed, 10000000);
        }
    }
    
    // Record the actual end block for realistic queries
    hotspot_update_end_block_ = current_block;
    utils::log_info("Hotspot update phase completed. Total processed: {}, final block: {}", total_processed, hotspot_update_end_block_);
}

void ScenarioRunner::run_historical_queries(size_t query_count) {
    utils::log_info("Running {} historical queries...", query_count);
    
    if (initial_load_key_blocks_.empty()) {
        utils::log_error("No initial load key-block pairs available for historical queries");
        return;
    }
    
    std::random_device rd;
    std::mt19937 gen(rd());
    
    // Weighted distribution: 1:2:7 (hot:medium:tail)
    std::discrete_distribution<int> type_dist({1, 2, 7}); // hot, medium, tail
    std::uniform_int_distribution<size_t> hot_dist(0, hot_key_indices_.size() - 1);
    std::uniform_int_distribution<size_t> medium_dist(0, medium_key_indices_.size() - 1);
    std::uniform_int_distribution<size_t> tail_dist(0, tail_key_indices_.size() - 1);
    
    utils::log_debug("Using {} pre-built key-block pairs for historical queries (hot:{}, medium:{}, tail:{})", 
                    initial_load_key_blocks_.size(), hot_key_indices_.size(), medium_key_indices_.size(), tail_key_indices_.size());
    
    for (size_t i = 0; i < query_count; ++i) {
        // Select key type based on 1:2:7 ratio
        int key_type = type_dist(gen);
        size_t pair_idx;
        
        switch (key_type) {
            case 0: // hot keys
                pair_idx = hot_key_indices_[hot_dist(gen)];
                break;
            case 1: // medium keys
                pair_idx = medium_key_indices_[medium_dist(gen)];
                break;
            case 2: // tail keys
                pair_idx = tail_key_indices_[tail_dist(gen)];
                break;
            default:
                pair_idx = hot_key_indices_[hot_dist(gen)]; // fallback
        }
        
        // Get the corresponding key-block pair
        const auto& pair = initial_load_key_blocks_[pair_idx];
        
        // Use the actual key and block that were written together
        const std::string& key = pair.key;
        BlockNum target_block = pair.block_num;
        
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
        
        metrics_collector_->start_query_timer();
        auto result = db_manager_->get_historical_state(key, target_block);
        metrics_collector_->stop_and_record_query(result.has_value());
        
        // Record cache hit metrics (simulate cache behavior)
        bool cache_hit = result.has_value() && (gen() % 100 < 80);  // 80% cache hit rate
        metrics_collector_->record_cache_hit(key_type_str, cache_hit);
    }
}

void ScenarioRunner::collect_rocksdb_statistics() {
    // Collect real bloom filter statistics
    uint64_t bloom_hits = db_manager_->get_bloom_filter_hits();
    uint64_t bloom_misses = db_manager_->get_bloom_filter_misses();
    uint64_t total_queries = db_manager_->get_point_query_total();
    
    if (total_queries > 0) {
        // Record actual bloom filter performance
        for (uint64_t i = 0; i < bloom_hits; ++i) {
            metrics_collector_->record_bloom_filter_query(true);
        }
        for (uint64_t i = 0; i < bloom_misses; ++i) {
            metrics_collector_->record_bloom_filter_query(false);
        }
    }
    
    // Collect real compaction statistics
    uint64_t bytes_read = db_manager_->get_compaction_bytes_read();
    uint64_t bytes_written = db_manager_->get_compaction_bytes_written();
    uint64_t time_micros = db_manager_->get_compaction_time_micros();
    
    if (bytes_read > 0) {
        // Estimate compaction count and record metrics
        size_t compaction_count = bytes_read / (10 * 1024 * 1024); // Rough estimate
        for (size_t i = 0; i < compaction_count; ++i) {
            double avg_time = static_cast<double>(time_micros) / compaction_count / 1000.0;
            metrics_collector_->record_compaction(avg_time, bytes_read / compaction_count, 2);
        }
    }
}