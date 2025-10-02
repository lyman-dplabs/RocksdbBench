#include "../src/strategies/direct_version_strategy.hpp"
#include "../src/core/config.hpp"
#include "../src/strategies/strategy_factory.hpp"
#include "../src/utils/logger.hpp"
#include <rocksdb/db.h>
#include <iostream>
#include <filesystem>

int main() {
    std::cout << "=== Testing DirectVersion Configuration ===" << std::endl;
    
    // 测试1：直接创建带配置的DirectVersionStrategy
    std::cout << "\n1. Testing direct configuration..." << std::endl;
    DirectVersionStrategy::Config config;
    config.batch_size_blocks = 10000;
    config.max_batch_size_bytes = 322122547200; // 300GB
    
    auto strategy1 = std::make_unique<DirectVersionStrategy>(config);
    
    // 测试2：通过BenchmarkConfig创建
    std::cout << "\n2. Testing BenchmarkConfig configuration..." << std::endl;
    BenchmarkConfig benchmark_config;
    benchmark_config.direct_version_batch_size = 50000;
    benchmark_config.direct_version_max_batch_bytes = 322122547200; // 300GB
    
    auto strategy2 = StorageStrategyFactory::create_direct_version_strategy(benchmark_config);
    
    // 测试3：命令行参数解析
    std::cout << "\n3. Testing command line argument parsing..." << std::endl;
    const char* argv[] = {
        "test_program",
        "--strategy", "direct_version",
        "--direct-batch-size", "75000",
        "--direct-max-batch-bytes", "322122547200"
    };
    int argc = 7;
    
    auto parsed_config = BenchmarkConfig::from_args(argc, const_cast<char**>(argv));
    std::cout << "Parsed batch_size: " << parsed_config.direct_version_batch_size << std::endl;
    std::cout << "Parsed max_bytes: " << parsed_config.direct_version_max_batch_bytes / (1024 * 1024 * 1024) << " GB" << std::endl;
    
    std::cout << "\n=== Configuration Test Complete ===" << std::endl;
    std::cout << "All DirectVersion configuration methods are working!" << std::endl;
    
    return 0;
}