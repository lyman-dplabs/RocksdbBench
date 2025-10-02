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
    utils::log_error("  1. Delete existing data and start fresh test");
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
    try {
        // Parse configuration from command line arguments
        auto config = BenchmarkConfig::from_args(argc, argv);
        
        // Initialize logger with strategy name and verbose setting
        utils::init_logger(config.storage_strategy, config.verbose);
        
        utils::log_info("RocksDB Historical Version Query Test Tool Starting...");
        config.print_config();
        
        // Create the storage strategy based on configuration
        auto strategy = StorageStrategyFactory::create_strategy(config.storage_strategy, config);
        
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
        
        // Create scenario runner with simplified config
        StrategyScenarioRunner runner(db_manager, metrics_collector, config);
        
        utils::log_info("Starting historical version query test...");
        utils::log_info("Test will run for {} minutes with {} keys", 
                       config.continuous_duration_minutes, config.total_keys);
        
        // 运行连续更新查询循环（这是唯一的模式）
        runner.run_continuous_update_query_loop(config.continuous_duration_minutes);
        
        utils::log_info("Historical version query test completed successfully!");
        
    } catch (const ConfigError& e) {
        utils::log_error("Configuration error: {}", e.what());
        BenchmarkConfig::print_help(argv[0]);
        return 1;
    } catch (const std::exception& e) {
        utils::log_error("Test failed with exception: {}", e.what());
        return 1;
    }
    
    return 0;
}