#include "core/config.hpp"
#include "core/strategy_db_manager.hpp"
#include "benchmark/strategy_scenario_runner.hpp"
#include "benchmark/metrics_collector.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"
#include <iostream>
#include <string>
#include <memory>

bool handle_existing_data(const std::string& db_path) {
    utils::log_error("Database data already exists at: {}", db_path);
    utils::log_error("Options:");
    utils::log_error("  1. Delete existing data and start fresh benchmark");
    utils::log_error("  2. Exit program");
    utils::log_error("Enter your choice (1 or 2): ");
    
    std::string choice;
    std::getline(std::cin, choice);
    
    if (choice == "1") {
        utils::log_info("Cleaning existing data...");
        return true;
    } else {
        utils::log_info("Exiting program as requested.");
        return false;
    }
}

int main(int argc, char* argv[]) {
    utils::log_info("RocksDB Benchmark Tool Starting...");
    
    try {
        // Parse configuration from command line arguments
        auto config = BenchmarkConfig::from_args(argc, argv);
        config.print_config();
        
        // Create the storage strategy based on configuration
        auto strategy = StorageStrategyFactory::create_strategy(config.storage_strategy);
        
        auto db_manager = std::make_shared<StrategyDBManager>(config.db_path, std::move(strategy));
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        // Configure bloom filter based on configuration
        db_manager->set_bloom_filter_enabled(config.enable_bloom_filter);
        
        bool should_clean = config.clean_existing_data;
        if (!should_clean && db_manager->data_exists()) {
            should_clean = handle_existing_data(config.db_path);
            if (!should_clean) {
                return 0;
            }
        }
        
        if (!db_manager->open(should_clean)) {
            utils::log_error("Failed to open database at path: {}", config.db_path);
            return 1;
        }
        
        utils::log_info("Database opened successfully at: {} with strategy: {}", 
                       config.db_path, config.storage_strategy);
        
        StrategyScenarioRunner runner(db_manager, metrics_collector);
        
        utils::log_info("Starting benchmark...");
        
        runner.run_initial_load_phase();
        
        runner.run_hotspot_update_phase();
        
        // Collect real RocksDB statistics before reporting
        runner.collect_rocksdb_statistics();
        
        metrics_collector->report_summary();
        
        utils::log_info("Benchmark completed successfully!");
        
    } catch (const ConfigError& e) {
        utils::log_error("Configuration error: {}", e.what());
        BenchmarkConfig::print_help(argv[0]);
        return 1;
    } catch (const std::exception& e) {
        utils::log_error("Benchmark failed with exception: {}", e.what());
        return 1;
    }
    
    return 0;
}