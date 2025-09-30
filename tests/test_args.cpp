#include <iostream>
#include <string>
#include <vector>
#include "core/config.hpp"

int main() {
    std::cout << "Testing argument parsing for RocksDB benchmark..." << std::endl;
    
    // Test cases
    std::vector<std::vector<const char*>> test_cases = {
        // Test 1: Basic dual strategy with custom path
        {"./rocksdb_bench_app", "--strategy", "dual_rocksdb_adaptive", "--db-path", "./test_db", "--initial-records", "1000", "--hotspot-updates", "500"},
        
        // Test 2: Default path with dual strategy
        {"./rocksdb_bench_app", "--strategy", "dual_rocksdb_adaptive", "--initial-records", "500", "--hotspot-updates", "200"},
        
        // Test 3: Dual strategy with clean flag
        {"./rocksdb_bench_app", "--strategy", "dual_rocksdb_adaptive", "--clean-data", "--initial-records", "100", "--hotspot-updates", "50"},
        
        // Test 4: Positional argument (legacy support)
        {"./rocksdb_bench_app", "./positional_db", "--strategy", "dual_rocksdb_adaptive", "--hotspot-updates", "100"},
        
        // Test 5: Help option
        {"./rocksdb_bench_app", "--help"}
    };
    
    for (size_t i = 0; i < test_cases.size(); ++i) {
        std::cout << "\n=== Test Case " << (i + 1) << " ===" << std::endl;
        
        const auto& args = test_cases[i];
        std::cout << "Command: ";
        for (const auto& arg : args) {
            std::cout << arg << " ";
        }
        std::cout << std::endl;
        
        try {
            int argc = static_cast<int>(args.size());
            auto config = BenchmarkConfig::from_args(argc, const_cast<char**>(args.data()));
            
            std::cout << "✓ Parsing succeeded!" << std::endl;
            std::cout << "  Strategy: " << config.storage_strategy << std::endl;
            std::cout << "  DB Path: " << config.db_path << std::endl;
            std::cout << "  Initial Records: " << config.initial_records << std::endl;
            std::cout << "  Clean Data: " << (config.clean_existing_data ? "true" : "false") << std::endl;
            
        } catch (const std::exception& e) {
            std::cout << "✗ Parsing failed: " << e.what() << std::endl;
        }
    }
    
    std::cout << "\n=== Testing run.sh script compatibility ===" << std::endl;
    
    // Test the exact command that run.sh would generate
    std::vector<const char*> runsh_test = {
        "./rocksdb_bench_app", 
        "--db-path", "./test_custom_path", 
        "--strategy", "dual_rocksdb_adaptive", 
        "--initial-records", "1000",
        "--hotspot-updates", "500"
    };
    
    std::cout << "Command (run.sh style): ";
    for (const auto& arg : runsh_test) {
        std::cout << arg << " ";
    }
    std::cout << std::endl;
    
    try {
        int argc = static_cast<int>(runsh_test.size());
        auto config = BenchmarkConfig::from_args(argc, const_cast<char**>(runsh_test.data()));
        
        std::cout << "✓ run.sh style parsing succeeded!" << std::endl;
        std::cout << "  Strategy: " << config.storage_strategy << std::endl;
        std::cout << "  DB Path: " << config.db_path << std::endl;
        std::cout << "  Initial Records: " << config.initial_records << std::endl;
        
    } catch (const std::exception& e) {
        std::cout << "✗ run.sh style parsing failed: " << e.what() << std::endl;
    }
    
    return 0;
}