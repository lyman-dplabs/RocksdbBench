#include "core/db_manager.hpp"
#include "benchmark/scenario_runner.hpp"
#include "benchmark/metrics_collector.hpp"
#include "utils/logger.hpp"
#include <iostream>
#include <string>
#include <memory>

int main(int argc, char* argv[]) {
    utils::log_info("RocksDB Benchmark Tool Starting...");
    
    std::string db_path = "./rocksdb_data";
    
    if (argc > 1) {
        db_path = argv[1];
    }
    
    try {
        auto db_manager = std::make_shared<DBManager>(db_path);
        auto metrics_collector = std::make_shared<MetricsCollector>();
        
        if (!db_manager->open()) {
            utils::log_error("Failed to open database at path: {}", db_path);
            return 1;
        }
        
        utils::log_info("Database opened successfully at: {}", db_path);
        
        ScenarioRunner runner(db_manager, metrics_collector);
        
        utils::log_info("Starting benchmark...");
        
        runner.run_initial_load_phase();
        
        runner.run_hotspot_update_phase();
        
        metrics_collector->report_summary();
        
        utils::log_info("Benchmark completed successfully!");
        
    } catch (const std::exception& e) {
        utils::log_error("Benchmark failed with exception: {}", e.what());
        return 1;
    }
    
    return 0;
}