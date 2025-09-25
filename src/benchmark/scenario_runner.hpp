#pragma once
#include "../core/db_manager.hpp"
#include "metrics_collector.hpp"
#include "../utils/data_generator.hpp"
#include <memory>

class ScenarioRunner {
public:
    ScenarioRunner(std::shared_ptr<DBManager> db, std::shared_ptr<MetricsCollector> metrics);

    void run_initial_load_phase();
    void run_hotspot_update_phase();
    
    // Collect real RocksDB statistics
    void collect_rocksdb_statistics();

    // Test helper method
    void run_historical_queries_test(size_t query_count) { run_historical_queries(query_count); }

private:
    std::shared_ptr<DBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    DataGenerator data_generator_;
    
    // Track actual block ranges for realistic queries
    BlockNum initial_load_end_block_ = 0;
    BlockNum hotspot_update_end_block_ = 0;
    
    // Track key-block pairs written during initial load to avoid reconstruction
    struct KeyBlockInfo {
        size_t key_idx;
        BlockNum block_num;
        std::string key;
    };
    std::vector<KeyBlockInfo> initial_load_key_blocks_;
    
    // Pre-classified key indices for efficient weighted selection
    std::vector<size_t> hot_key_indices_;
    std::vector<size_t> medium_key_indices_;
    std::vector<size_t> tail_key_indices_;
    
    void run_historical_queries(size_t query_count);
};