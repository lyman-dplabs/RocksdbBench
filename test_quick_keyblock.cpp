#include "src/benchmark/scenario_runner.hpp"
#include "src/core/db_manager.hpp"
#include "src/benchmark/metrics_collector.hpp"
#include "src/utils/logger.hpp"
#include <cassert>
#include <chrono>

int main() {
    utils::log_info("Quick test of key-block pairing fix...");
    
    std::string db_path = "/tmp/test_quick_keyblock";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Run initial load phase
    utils::log_info("Starting initial load phase...");
    runner.run_initial_load_phase();
    
    auto load_end = std::chrono::high_resolution_clock::now();
    auto load_duration = std::chrono::duration_cast<std::chrono::seconds>(load_end - start_time);
    utils::log_info("Initial load completed in {} seconds", load_duration.count());
    
    // Run smaller batch of queries for quick test
    const size_t query_count = 1000;
    utils::log_info("Running {} historical queries...", query_count);
    
    auto query_start = std::chrono::high_resolution_clock::now();
    runner.run_historical_queries_test(query_count);
    auto query_end = std::chrono::high_resolution_clock::now();
    auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(query_end - query_start);
    
    // Check results
    const auto& query_metrics = metrics_collector->get_query_metrics();
    double success_rate = query_metrics.total_queries > 0 ? 
        (static_cast<double>(query_metrics.successful_queries) / query_metrics.total_queries) * 100 : 0;
    
    utils::log_info("Query results:");
    utils::log_info("  Total queries: {}", query_metrics.total_queries);
    utils::log_info("  Successful: {}", query_metrics.successful_queries);
    utils::log_info("  Success rate: {:.2f}%", success_rate);
    utils::log_info("  Query time: {} ms", query_duration.count());
    utils::log_info("  Avg query time: {:.2f} ms", query_metrics.total_queries > 0 ? 
        static_cast<double>(query_duration.count()) / query_metrics.total_queries : 0.0);
    
    // Key distribution
    const auto& cache_metrics = metrics_collector->get_cache_hit_metrics();
    utils::log_info("Key distribution:");
    utils::log_info("  Hot keys: {}", cache_metrics.hot_key_queries);
    utils::log_info("  Medium keys: {}", cache_metrics.medium_key_queries);
    utils::log_info("  Tail keys: {}", cache_metrics.tail_key_queries);
    
    if (success_rate >= 90.0) {
        utils::log_info("✅ SUCCESS: Key-block pairing fix is working!");
    } else {
        utils::log_error("❌ FAILURE: Success rate too low: {:.2f}%", success_rate);
    }
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    return 0;
}