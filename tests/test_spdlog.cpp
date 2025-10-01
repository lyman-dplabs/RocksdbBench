#include "../src/utils/logger.hpp"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    // Test basic logging functionality
    std::cout << "Testing spdlog functionality...\n";
    
    // Initialize logger with test strategy name
    utils::init_logger("test_strategy");
    
    utils::log_info("This is a test info message");
    utils::log_error("This is a test error message");
    utils::log_debug("This is a test debug message");
    utils::log_warn("This is a test warning message");
    
    // Test formatted logging
    int value = 42;
    double pi = 3.14159;
    utils::log_info("Testing formatted output: value={}, pi={:.2f}", value, pi);
    
    // Test log file creation
    utils::log_info("Log file should be created at: logs/test_strategy_[timestamp].log");
    
    // Test flush functionality
    utils::log_info_flush("This message should be flushed immediately");
    
    std::cout << "Test completed. Check logs/ directory for log files.\n";
    
    return 0;
}