#include "src/benchmark/scenario_runner.hpp"
#include "src/core/db_manager.hpp"
#include "src/benchmark/metrics_collector.hpp"
#include "src/utils/logger.hpp"
#include <cassert>

void test_historical_query_success_rate() {
    utils::log_info("Testing historical query success rate...");
    
    // Create temporary database path
    std::string db_path = "/tmp/test_historical_queries";
    
    // Initialize components
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    // Run initial load phase with larger dataset for testing
    utils::log_info("Running initial load phase...");
    runner.run_initial_load_phase();
    
    // Run historical queries with larger count to test key-block pairing
    const size_t query_count = 10000;
    utils::log_info("Running {} historical queries...", query_count);
    runner.run_historical_queries_test(query_count);
    
    // Check metrics
    const auto& query_metrics = metrics_collector->get_query_metrics();
    double success_rate = query_metrics.total_queries > 0 ? 
        (static_cast<double>(query_metrics.successful_queries) / query_metrics.total_queries) * 100 : 0;
    
    utils::log_info("Query success rate: {:.2f}%", success_rate);
    utils::log_info("Successful queries: {}", query_metrics.successful_queries);
    utils::log_info("Total queries: {}", query_metrics.total_queries);
    
    // Assert that success rate is at least 90%
    assert(success_rate >= 90.0);
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    utils::log_info("Historical query success rate test passed!");
}

void test_bloom_filter_metrics() {
    utils::log_info("Testing Bloom filter metrics collection...");
    
    std::string db_path = "/tmp/test_bloom_filter";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    // Run initial load to generate data and queries
    runner.run_initial_load_phase();
    runner.run_historical_queries_test(500);
    
    // Collect RocksDB statistics
    runner.collect_rocksdb_statistics();
    
    // Check bloom filter metrics
    const auto& bloom_metrics = metrics_collector->get_bloom_filter_metrics();
    utils::log_info("Bloom filter hits: {}", bloom_metrics.bloom_filter_hits);
    utils::log_info("Bloom filter misses: {}", bloom_metrics.bloom_filter_misses);
    utils::log_info("Total point queries: {}", bloom_metrics.total_point_queries);
    
    // With bloom filter enabled, we should see some metrics
    // (Note: actual values depend on RocksDB internals)
    utils::log_info("Bloom filter metrics test completed");
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
}

void test_key_block_pairing() {
    utils::log_info("Testing key-block pairing logic...");
    
    std::string db_path = "/tmp/test_key_block";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    // Run initial load with small dataset
    runner.run_initial_load_phase();
    
    // Test a few specific queries that should succeed
    const size_t test_queries = 10;
    runner.run_historical_queries_test(test_queries);
    
    // All queries should succeed with proper key-block pairing
    const auto& query_metrics = metrics_collector->get_query_metrics();
    double test_success_rate = query_metrics.total_queries > 0 ? 
        (static_cast<double>(query_metrics.successful_queries) / query_metrics.total_queries) * 100 : 0;
    
    utils::log_info("Key-block pairing test success rate: {:.2f}%", test_success_rate);
    assert(test_success_rate == 100.0);
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    utils::log_info("Key-block pairing test passed!");
}

void test_large_keyspace_queries() {
    utils::log_info("Testing larger keyspace queries...");
    
    std::string db_path = "/tmp/test_large_keyspace";
    
    auto db_manager = std::make_shared<DBManager>(db_path);
    auto metrics_collector = std::make_shared<MetricsCollector>();
    ScenarioRunner runner(db_manager, metrics_collector);
    
    // Clean and open database
    assert(db_manager->clean_data());
    assert(db_manager->open(true));
    
    // Run initial load phase (this will load 100M keys by default)
    utils::log_info("Running initial load phase with 100M keys...");
    runner.run_initial_load_phase();
    
    // Run a larger number of historical queries to test across the expanded keyspace
    const size_t query_count = 50000;
    utils::log_info("Running {} historical queries across large keyspace...", query_count);
    runner.run_historical_queries_test(query_count);
    
    // Check metrics
    const auto& query_metrics = metrics_collector->get_query_metrics();
    double success_rate = query_metrics.total_queries > 0 ? 
        (static_cast<double>(query_metrics.successful_queries) / query_metrics.total_queries) * 100 : 0;
    
    utils::log_info("Large keyspace query success rate: {:.2f}%", success_rate);
    utils::log_info("Successful queries: {}", query_metrics.successful_queries);
    utils::log_info("Total queries: {}", query_metrics.total_queries);
    
    // With proper key-block pairing, success rate should still be high
    assert(success_rate >= 90.0);
    
    // Test distribution across different key types
    const auto& cache_metrics = metrics_collector->get_cache_hit_metrics();
    utils::log_info("Hot key queries: {}", cache_metrics.hot_key_queries);
    utils::log_info("Medium key queries: {}", cache_metrics.medium_key_queries);
    utils::log_info("Tail key queries: {}", cache_metrics.tail_key_queries);
    
    // Clean up
    db_manager->close();
    db_manager->clean_data();
    
    utils::log_info("Large keyspace test passed!");
}

int main() {
    try {
        utils::log_info("Starting unit tests for historical query fixes...");
        
        test_historical_query_success_rate();
        test_bloom_filter_metrics();
        test_key_block_pairing();
        test_large_keyspace_queries();
        
        utils::log_info("All unit tests passed!");
        return 0;
    } catch (const std::exception& e) {
        utils::log_error("Unit test failed: {}", e.what());
        return 1;
    }
}