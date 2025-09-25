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
    
    void run_historical_queries(size_t query_count);
};