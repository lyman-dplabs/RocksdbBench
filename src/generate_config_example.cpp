#include <CLI/CLI.hpp>
#include <nlohmann/json.hpp>
#include <iostream>
#include <fstream>

using json = nlohmann::json;

int main() {
    
    // 创建JSON配置示例
    json config_json;
    config_json["database_path"] = "./rocksdb_data";
    config_json["storage_strategy"] = "dual_rocksdb_adaptive";
    config_json["clean_start"] = true;
    config_json["initial_records"] = 100000000;
    config_json["hotspot_updates"] = 10000000;
    
    // 添加策略特定配置
    json strategy_config;
    strategy_config["dual_rocksdb_adaptive"] = {
        {"range_size", 10000},
        {"max_cache_memory_mb", 1024},
        {"hot_cache_ratio", 0.01},
        {"medium_cache_ratio", 0.05},
        {"enable_compression", true},
        {"enable_bloom_filters", true},
        {"enable_dynamic_cache_optimization", false},
        {"enable_sharding", false},
        {"expected_key_count", 0}
    };
    
    strategy_config["page_index"] = {
        {"page_size", 1000}
    };
    
    strategy_config["direct_version"] = {
        {"compression_enabled", true}
    };
    
    config_json["strategy_config"] = strategy_config;
    
    // 输出到标准输出
    std::cout << config_json.dump(4) << std::endl;
    return 0;
}