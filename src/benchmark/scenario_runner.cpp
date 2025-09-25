#include "scenario_runner.hpp"
#include "../utils/logger.hpp"
#include <random>
#include <algorithm>

ScenarioRunner::ScenarioRunner(std::shared_ptr<DBManager> db, std::shared_ptr<MetricsCollector> metrics)
    : db_manager_(db), metrics_collector_(metrics), data_generator_(DataGenerator::Config()) {}

void ScenarioRunner::run_initial_load_phase() {
    utils::log_info("Starting initial load phase...");
    
    const auto& all_keys = data_generator_.get_all_keys();
    const size_t batch_size = 10000;
    size_t total_keys = all_keys.size();
    BlockNum current_block = 0;
    
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
    
    utils::log_info("Initial load phase completed. Total blocks written: {}", current_block);
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
    
    utils::log_info("Hotspot update phase completed. Total processed: {}", total_processed);
}

void ScenarioRunner::run_historical_queries(size_t query_count) {
    utils::log_info("Running {} historical queries...", query_count);
    
    const auto& all_keys = data_generator_.get_all_keys();
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<size_t> key_dist(0, all_keys.size() - 1);
    
    // Calculate actual block range based on written data
    BlockNum max_init_block = 100000000 / 10000;  // ~10000 blocks from initial load
    BlockNum max_update_block = max_init_block + (10000000 / 10000);  // +1000 blocks from updates
    std::uniform_int_distribution<BlockNum> block_dist(0, max_update_block);
    
    utils::log_debug("Query block range: 0 to {}", max_update_block);
    
    for (size_t i = 0; i < query_count; ++i) {
        size_t key_idx = key_dist(gen);
        BlockNum target_block = block_dist(gen);
        
        metrics_collector_->start_query_timer();
        auto result = db_manager_->get_historical_state(all_keys[key_idx], target_block);
        metrics_collector_->stop_and_record_query(result.has_value());
    }
}