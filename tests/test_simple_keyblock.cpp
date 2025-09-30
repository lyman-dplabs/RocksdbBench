#include "benchmark/scenario_runner.hpp"
#include "core/db_manager.hpp"
#include "benchmark/metrics_collector.hpp"
#include "utils/logger.hpp"
#include <cassert>

int main() {
    utils::log_info("Testing key-block pairing logic with simplified dataset...");
    
    std::string db_path = "/tmp/test_simple_keyblock";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    // Run initial load phase (100M keys by default)
    utils::log_info("Running initial load phase...");
    runner.run_initial_load_phase();
    
    // Run smaller set of historical queries to test
    const size_t query_count = 1000;
    utils::log_info("Running {} historical queries...", query_count);
    runner.run_historical_queries_test(query_count);
    
    // Check metrics
    const auto& query_metrics = metrics_collector->get_query_metrics();
    double success_rate = query_metrics.total_queries > 0 ? 
        (static_cast<double>(query_metrics.successful_queries) / query_metrics.total_queries) * 100 : 0;
    
    utils::log_info("Query success rate: {:.2f}%", success_rate);
    utils::log_info("Successful queries: {}/{}", query_metrics.successful_queries, query_metrics.total_queries);
    
    // Check key distribution
    const auto& cache_metrics = metrics_collector->get_cache_hit_metrics();
    utils::log_info("Hot key queries: {}", cache_metrics.hot_key_queries);
    utils::log_info("Medium key queries: {}", cache_metrics.medium_key_queries);
    utils::log_info("Tail key queries: {}", cache_metrics.tail_key_queries);
    
    utils::log_info("Test completed. Success rate: {:.2f}%", success_rate);
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    return 0;
}