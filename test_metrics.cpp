#include "src/core/db_manager.hpp"
#include "src/benchmark/metrics_collector.hpp"
#include "src/utils/logger.hpp"
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <chrono>

int main() {
    utils::log_info("Testing metrics collection...");
    
    std::string db_path = "./test_metrics_db";
    
    try {
        auto db_manager = std::make_shared<DBManager>(db_path);
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        // Clean and open database
        if (db_manager->data_exists()) {
            db_manager->clean_data();
        }
        
        if (!db_manager->open(false)) {
            utils::log_error("Failed to open database");
            return 1;
        }
        
        // Set merge callback
        db_manager->set_merge_callback([metrics_collector](size_t merged_values, size_t merged_value_size) {
            metrics_collector->record_merge_operation(merged_values, merged_value_size);
        });
        
        utils::log_info("Database opened successfully");
        
        // Write some test data
        std::vector<ChangeSetRecord> changes = {
            {1, "test_key_1", "value1"},
            {1, "test_key_2", "value2"}
        };
        
        std::vector<IndexRecord> indices = {
            {0, "test_key_1", {1}},
            {0, "test_key_2", {1}}
        };
        
        // Record write metrics
        metrics_collector->start_write_timer();
        bool success = db_manager->write_batch(changes, indices);
        metrics_collector->stop_and_record_write(changes.size(), changes.size() * 50);
        
        if (success) {
            utils::log_info("Test data written successfully");
        }
        
        // Test queries - use a more realistic block range
        BlockNum max_block = 10; // We wrote 2 batches, so blocks 0 and 1
        for (int i = 0; i < 10; i++) {
            // Query blocks that actually have data
            BlockNum target_block = (i % 2); // Alternate between blocks 0 and 1
            
            metrics_collector->start_query_timer();
            auto result = db_manager->get_historical_state("test_key_1", target_block);
            metrics_collector->stop_and_record_query(result.has_value());
            
            // Record some cache hits
            metrics_collector->record_cache_hit("hot", i % 3 == 0);
        }
        
        // Record some bloom filter metrics (simulated for testing)
        for (int i = 0; i < 5; i++) {
            metrics_collector->record_bloom_filter_query(true);
        }
        for (int i = 0; i < 2; i++) {
            metrics_collector->record_bloom_filter_query(false);
        }
        
        // Record some compaction metrics (simulated for testing)
        metrics_collector->record_compaction(10.5, 1024 * 1024, 2);
        
        utils::log_info("Metrics collection test completed");
        metrics_collector->report_summary();
        
    } catch (const std::exception& e) {
        utils::log_error("Test failed: {}", e.what());
        return 1;
    }
    
    return 0;
}
