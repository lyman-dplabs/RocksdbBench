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

private:
    std::shared_ptr<DBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    DataGenerator data_generator_;
    
    // Track actual block ranges for realistic queries
    BlockNum initial_load_end_block_ = 0;
    BlockNum hotspot_update_end_block_ = 0;
    
    // Store the keys that were actually written in initial load phase
    std::vector<std::string> initial_load_keys_;
    
    void run_historical_queries(size_t query_count);
};