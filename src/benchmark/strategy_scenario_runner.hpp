#pragma once
#include "../core/strategy_db_manager.hpp"
#include "metrics_collector.hpp"
#include "../utils/data_generator.hpp"
#include "../core/config.hpp"
#include <memory>

class StrategyScenarioRunner {
public:
    StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager, 
                         std::shared_ptr<MetricsCollector> metrics,
                         const BenchmarkConfig& config);

    void run_initial_load_phase();
    void run_hotspot_update_phase();
    
    // Collect real RocksDB statistics
    void collect_rocksdb_statistics();
    
    // Get current strategy information
    std::string get_current_strategy() const;
    
    // Test helper method
    void run_historical_queries_test(size_t query_count) { run_historical_queries(query_count); }

private:
    std::shared_ptr<StrategyDBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    std::unique_ptr<DataGenerator> data_generator_;
    BenchmarkConfig config_;
    
    // Track actual block ranges for realistic queries
    BlockNum initial_load_end_block_ = 0;
    BlockNum hotspot_update_end_block_ = 0;
    
      
    void run_historical_queries(size_t query_count);
};