#include <iostream>
#include <memory>
#include "core/config.hpp"
#include "utils/logger.hpp"
#include "strategies/strategy_factory.hpp"

int main() {
    std::cout << "Testing DualRocksDBStrategy..." << std::endl;
    
    try {
        // Test strategy factory
        auto strategies = StorageStrategyFactory::get_available_strategies();
        std::cout << "Available strategies: " << std::endl;
        for (const auto& strategy : strategies) {
            std::cout << "  - " << strategy << std::endl;
        }
        
        // Test creating the dual rocksdb strategy
        std::cout << "\nCreating DualRocksDBStrategy..." << std::endl;
        auto dual_strategy = StorageStrategyFactory::create_strategy("dual_rocksdb_adaptive");
        if (dual_strategy) {
            std::cout << "✓ DualRocksDBStrategy created successfully!" << std::endl;
            std::cout << "Strategy name: " << dual_strategy->get_strategy_name() << std::endl;
            std::cout << "Description: " << dual_strategy->get_description() << std::endl;
        } else {
            std::cout << "✗ Failed to create DualRocksDBStrategy" << std::endl;
            return 1;
        }
        
        std::cout << "\nTest passed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    }
}