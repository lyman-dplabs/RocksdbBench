#pragma once
#include "../core/strategy_db_manager.hpp"
#include "metrics_collector.hpp"
#include "../utils/data_generator.hpp"
#include "../core/config.hpp"
#include <memory>
#include <chrono>
#include <vector>
#include <algorithm>

class StrategyScenarioRunner {
public:
    StrategyScenarioRunner(std::shared_ptr<StrategyDBManager> db_manager, 
                         std::shared_ptr<MetricsCollector> metrics,
                         const BenchmarkConfig& config);

    void run_initial_load_phase();
    
    // New update-query loop that runs for specified duration
    void run_continuous_update_query_loop(size_t duration_minutes = 360);
    
    // Collect real RocksDB statistics
    void collect_rocksdb_statistics();
    
    // Get current strategy information
    std::string get_current_strategy() const;
    

private:
    std::shared_ptr<StrategyDBManager> db_manager_;
    std::shared_ptr<MetricsCollector> metrics_collector_;
    std::unique_ptr<DataGenerator> data_generator_;
    BenchmarkConfig config_;
    
    // Track actual block ranges for realistic queries
    BlockNum initial_load_end_block_ = 0;
    BlockNum hotspot_update_end_block_ = 0;
    BlockNum current_max_block_ = 0;
    
    // Performance tracking
    struct PerformanceMetrics {
        std::vector<double> write_latencies_ms;
        std::vector<double> query_latencies_ms;
        std::chrono::steady_clock::time_point start_time;
        std::chrono::steady_clock::time_point end_time;
        
        void print_statistics() const;
    };
    PerformanceMetrics perf_metrics_;
    
    // New query interface for random historical version queries
    struct QueryResult {
        bool found;
        BlockNum block_num;
        Value value;
        double latency_ms;
    };
    
    QueryResult query_historical_version(const std::string& addr_slot, BlockNum target_version);
    void run_single_block_update_query(size_t block_num);
    void log_performance_metrics(const std::string& operation_type, double latency_ms);
};