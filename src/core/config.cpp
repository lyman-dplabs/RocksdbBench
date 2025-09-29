#include "config.hpp"
#include "../strategies/page_index_strategy.hpp"
#include "../strategies/direct_version_strategy.hpp"
#include "../strategies/dual_rocksdb_strategy.hpp"
#include "../utils/logger.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <algorithm>
#include <cctype>

// 策略工厂实现
std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_strategy(
    const std::string& strategy_type) {
    
    std::string normalized_type = strategy_type;
    // 转换为小写以支持大小写不敏感
    std::transform(normalized_type.begin(), normalized_type.end(), 
                  normalized_type.begin(), ::tolower);
    
    if (normalized_type == "page_index" || normalized_type == "pageindex") {
        return create_page_index_strategy();
    } else if (normalized_type == "direct_version" || normalized_type == "directversion") {
        return create_direct_version_strategy();
    } else if (normalized_type == "dual_rocksdb_adaptive" || normalized_type == "dualrocksdbadaptive") {
        return create_dual_rocksdb_strategy();
    } else if (normalized_type == "simple_keyblock" || normalized_type == "simplekeyblock") {
        // TODO: 实现SimpleKeyBlockStrategy
        throw std::runtime_error("Strategy 'simple_keyblock' not yet implemented");
    } else if (normalized_type == "reduced_keyblock" || normalized_type == "reducedkeyblock") {
        // TODO: 实现ReducedKeyBlockStrategy
        throw std::runtime_error("Strategy 'reduced_keyblock' not yet implemented");
    }
    
    throw std::runtime_error("Unknown storage strategy: " + strategy_type);
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_page_index_strategy() {
    // 创建PageIndexStrategy，设置merge callback
    auto strategy = std::make_unique<PageIndexStrategy>(nullptr);
    return strategy;
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_direct_version_strategy() {
    return std::make_unique<DirectVersionStrategy>();
}

std::unique_ptr<IStorageStrategy> StorageStrategyFactory::create_dual_rocksdb_strategy() {
    DualRocksDBStrategy::Config config;
    // 使用默认配置，可以根据需要从命令行参数或配置文件读取
    return std::make_unique<DualRocksDBStrategy>(config);
}

std::vector<std::string> StorageStrategyFactory::get_available_strategies() {
    return {"page_index", "direct_version", "dual_rocksdb_adaptive", "simple_keyblock", "reduced_keyblock"};
}

void StorageStrategyFactory::print_available_strategies() {
    utils::log_info("Available storage strategies:");
    auto strategies = get_available_strategies();
    for (const auto& strategy : strategies) {
        utils::log_info("  - {}", strategy);
    }
}

// BenchmarkConfig实现
BenchmarkConfig BenchmarkConfig::from_args(int argc, char* argv[]) {
    BenchmarkConfig config;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        if (arg == "--help" || arg == "-h") {
            print_help(argv[0]);
            exit(0);
        } else if (arg.starts_with("--strategy=")) {
            config.storage_strategy = arg.substr(10);
        } else if (arg == "--strategy" && i + 1 < argc) {
            config.storage_strategy = argv[++i];
        } else if (arg.starts_with("--db_path=")) {
            config.db_path = arg.substr(10);
        } else if (arg == "--db_path" && i + 1 < argc) {
            config.db_path = argv[++i];
        } else if (arg.starts_with("--initial_records=")) {
            config.initial_records = std::stoull(arg.substr(17));
        } else if (arg == "--initial_records" && i + 1 < argc) {
            config.initial_records = std::stoull(argv[++i]);
        } else if (arg.starts_with("--hotspot_updates=")) {
            config.hotspot_updates = std::stoull(arg.substr(17));
        } else if (arg == "--hotspot_updates" && i + 1 < argc) {
            config.hotspot_updates = std::stoull(argv[++i]);
        } else if (arg.starts_with("--query_interval=")) {
            config.query_interval = std::stoull(arg.substr(15));
        } else if (arg == "--query_interval" && i + 1 < argc) {
            config.query_interval = std::stoull(argv[++i]);
        } else if (arg == "--disable_bloom_filter") {
            config.enable_bloom_filter = false;
        } else if (arg == "--clean_data") {
            config.clean_existing_data = true;
        } else if (arg.starts_with("--config=")) {
            std::string config_path = arg.substr(9);
            return from_file(config_path);
        } else if (!arg.starts_with("--")) {
            // 位置参数：数据库路径
            config.db_path = arg;
        } else {
            throw ConfigError("Unknown argument: " + arg);
        }
    }
    
    return config;
}

BenchmarkConfig BenchmarkConfig::from_file(const std::string& config_path) {
    std::ifstream file(config_path);
    if (!file.is_open()) {
        throw ConfigError("Cannot open config file: " + config_path);
    }
    
    BenchmarkConfig config;
    std::string line;
    std::string section;
    
    while (std::getline(file, line)) {
        // 移除注释和空白
        line = line.substr(0, line.find('#'));
        line.erase(0, line.find_first_not_of(" \t"));
        line.erase(line.find_last_not_of(" \t") + 1);
        
        if (line.empty()) continue;
        
        // 处理section头
        if (line[0] == '[' && line[line.length() - 1] == ']') {
            section = line.substr(1, line.length() - 2);
            continue;
        }
        
        // 处理key=value
        size_t equal_pos = line.find('=');
        if (equal_pos != std::string::npos) {
            std::string key = line.substr(0, equal_pos);
            std::string value = line.substr(equal_pos + 1);
            
            // 移除空白
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            value.erase(value.find_last_not_of(" \t") + 1);
            
            if (section.empty() || section == "benchmark") {
                if (key == "storage_strategy") {
                    config.storage_strategy = value;
                } else if (key == "db_path") {
                    config.db_path = value;
                } else if (key == "initial_records") {
                    config.initial_records = std::stoull(value);
                } else if (key == "hotspot_updates") {
                    config.hotspot_updates = std::stoull(value);
                } else if (key == "query_interval") {
                    config.query_interval = std::stoull(value);
                } else if (key == "enable_bloom_filter") {
                    config.enable_bloom_filter = (value == "true" || value == "1");
                } else if (key == "clean_existing_data") {
                    config.clean_existing_data = (value == "true" || value == "1");
                }
            }
        }
    }
    
    return config;
}

void BenchmarkConfig::print_config() const {
    utils::log_info("=== Benchmark Configuration ===");
    utils::log_info("Storage Strategy: {}", storage_strategy);
    utils::log_info("Database Path: {}", db_path);
    utils::log_info("Initial Records: {}", initial_records);
    utils::log_info("Hotspot Updates: {}", hotspot_updates);
    utils::log_info("Query Interval: {}", query_interval);
    utils::log_info("Bloom Filter: {}", enable_bloom_filter ? "Enabled" : "Disabled");
    utils::log_info("Clean Existing Data: {}", clean_existing_data ? "Yes" : "No");
    utils::log_info("==============================");
}

void BenchmarkConfig::print_help(const std::string& program_name) {
    std::cout << "Usage: " << program_name << " [options] [db_path]\n";
    std::cout << "\nOptions:\n";
    std::cout << "  --strategy STRATEGY          Storage strategy to use (page_index, direct_version)\n";
    std::cout << "  --db_path PATH               Database path (default: ./rocksdb_data)\n";
    std::cout << "  --initial_records N          Number of initial records (default: 100000000)\n";
    std::cout << "  --hotspot_updates N          Number of hotspot updates (default: 10000000)\n";
    std::cout << "  --query_interval N           Query interval (default: 500000)\n";
    std::cout << "  --disable_bloom_filter       Disable bloom filter\n";
    std::cout << "  --clean_data                 Clean existing data before starting\n";
    std::cout << "  --config FILE                Load configuration from file\n";
    std::cout << "  -h, --help                    Show this help message\n";
    std::cout << "\nExamples:\n";
    std::cout << "  " << program_name << " --strategy direct_version\n";
    std::cout << "  " << program_name << " /path/to/db --strategy page_index\n";
    std::cout << "  " << program_name << " --config benchmark_config.json\n";
}