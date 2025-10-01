#include "config.hpp"
#include "../utils/logger.hpp"
#include <CLI/CLI.hpp>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <nlohmann/json.hpp>
#include <sstream>

// 使用json库进行配置文件读写
using json = nlohmann::json;

// BenchmarkConfig实现 - 使用CLI11
BenchmarkConfig BenchmarkConfig::from_args(int argc, char *argv[]) {
  BenchmarkConfig config;

  CLI::App app{"RocksDB Benchmark Tool - Modern Command Line Interface"};

  // 基本选项
  app.add_option("-s,--strategy", config.storage_strategy,
                 "Storage strategy to use (page_index, direct_version, "
                 "dual_rocksdb_adaptive)")
      ->check(CLI::IsMember({"page_index", "direct_version",
                             "dual_rocksdb_adaptive", "simple_keyblock",
                             "reduced_keyblock"}))
      ->default_val("page_index");

  app.add_option("-d,--db-path", config.db_path, "Database path")
      ->default_val("./rocksdb_data");

  app.add_option("-i,--initial-records", config.initial_records,
                 "Number of initial records")
      ->default_val(100000000)
      ->check(CLI::PositiveNumber);

  app.add_option("-u,--hotspot-updates", config.hotspot_updates,
                 "Number of hotspot updates")
      ->default_val(10000000)
      ->check(CLI::PositiveNumber);

  // 布尔选项
  app.add_flag("--disable-bloom-filter", config.enable_bloom_filter,
               "Disable bloom filter (default: enabled)")
      ->default_val(true);

  app.add_flag("-c,--clean-data", config.clean_existing_data,
               "Clean existing data before starting");

  app.add_flag("-v,--verbose", config.verbose, "Enable verbose output");

  app.add_flag("--enable-compression", config.enable_compression,
               "Enable compression for all strategies that support it");

  
  // DualRocksDB特定配置
  auto *dual_group = app.add_option_group(
      "DualRocksDB Options",
      "Options specific to dual_rocksdb_adaptive strategy");

  dual_group
      ->add_option("--dual-range-size", config.dual_rocksdb_range_size,
                   "Range size for DualRocksDB strategy")
      ->default_val(10000)
      ->check(CLI::PositiveNumber);

  dual_group
      ->add_option("--dual-cache-size", config.dual_rocksdb_cache_size,
                   "Cache size in bytes for DualRocksDB strategy")
      ->default_val(1024 * 1024 * 1024)
      ->check(CLI::PositiveNumber);

  dual_group
      ->add_option("--dual-hot-ratio", config.dual_rocksdb_hot_ratio,
                   "Hot cache ratio for DualRocksDB strategy")
      ->default_val(0.01)
      ->check(CLI::Range(0.0, 1.0));

  dual_group
      ->add_option("--dual-medium-ratio", config.dual_rocksdb_medium_ratio,
                   "Medium cache ratio for DualRocksDB strategy")
      ->default_val(0.05)
      ->check(CLI::Range(0.0, 1.0));

  dual_group
      ->add_flag("--dual-enable-dynamic-cache", config.dual_rocksdb_dynamic_cache,
                 "Enable dynamic cache optimization for DualRocksDB strategy");

  
  dual_group
      ->add_option("--dual-batch-size", config.dual_rocksdb_batch_size,
                   "Number of blocks per write batch for DualRocksDB strategy")
      ->default_val(5)
      ->check(CLI::PositiveNumber);

  dual_group
      ->add_option("--dual-max-batch-bytes", config.dual_rocksdb_max_batch_bytes,
                   "Maximum batch size in bytes for DualRocksDB strategy")
      ->default_val(128 * 1024 * 1024)
      ->check(CLI::PositiveNumber);

  
  // 位置参数
  app.add_option("db_path_pos", config.db_path,
                 "Database path (positional argument)")
      ->expected(0, 1);

  // 设置帮助信息格式
  app.set_help_flag("-h,--help", "Show help message");
  
  // 自定义版本选项
  bool version_flag = false;
  app.add_flag("--version", version_flag, "Show version information");

  // 验证回调
  app.callback([&config]() {
    if (!config.validate()) {
      auto errors = config.get_validation_errors();
      for (const auto &error : errors) {
        std::cerr << "Error: " << error << std::endl;
      }
      throw CLI::ValidationError("Configuration validation failed");
    }
  });

  try {
    // 使用CLI11_PARSE_AND_THROW，如果失败会抛出异常
    app.parse(argc, argv);

    // 检查版本标志
    if (version_flag) {
      print_version_info();
      std::exit(0);
    }

    // 配置文件功能暂时禁用，因为实现不完整
    // if (!config.config_file.empty()) {
    //   auto file_config = from_file(config.config_file);
    //   // 命令行参数优先级高于配置文件
    //   if (config.storage_strategy ==
    //       "page_index") { // 默认值，可能被配置文件覆盖
    //     config.storage_strategy = file_config.storage_strategy;
    //   }
    //   if (config.db_path == "./rocksdb_data") { // 默认值，可能被配置文件覆盖
    //     config.db_path = file_config.db_path;
    //   }
    //   // 其他字段的合并逻辑...
    // }

  } catch (const CLI::ParseError &e) {
    std::cerr << "Parse error: " << e.what() << std::endl;
    if (e.get_exit_code() != 0) {
      exit(e.get_exit_code());
    }
    throw ConfigError("Parse error: " + std::string(e.what()));
  } catch (const CLI::Error &e) {
    std::cerr << "CLI error: " << e.what() << std::endl;
    throw ConfigError("CLI error: " + std::string(e.what()));
  }

  return config;
}

BenchmarkConfig BenchmarkConfig::from_file(const std::string &config_path) {
  std::ifstream file(config_path);
  if (!file.is_open()) {
    throw ConfigError("Cannot open config file: " + config_path);
  }

  try {
    json j;
    file >> j;

    BenchmarkConfig config;

    if (j.contains("benchmark")) {
      auto &bench = j["benchmark"];
      if (bench.contains("storage_strategy"))
        config.storage_strategy = bench["storage_strategy"].get<std::string>();
      if (bench.contains("db_path"))
        config.db_path = bench["db_path"].get<std::string>();
      if (bench.contains("initial_records"))
        config.initial_records = bench["initial_records"].get<size_t>();
      if (bench.contains("hotspot_updates"))
        config.hotspot_updates = bench["hotspot_updates"].get<size_t>();
      if (bench.contains("enable_bloom_filter"))
        config.enable_bloom_filter = bench["enable_bloom_filter"].get<bool>();
      if (bench.contains("enable_compression"))
        config.enable_compression = bench["enable_compression"].get<bool>();
      if (bench.contains("clean_existing_data"))
        config.clean_existing_data = bench["clean_existing_data"].get<bool>();
      if (bench.contains("verbose"))
        config.verbose = bench["verbose"].get<bool>();
    }

    if (j.contains("dual_rocksdb")) {
      auto &dual = j["dual_rocksdb"];
      if (dual.contains("range_size"))
        config.dual_rocksdb_range_size = dual["range_size"].get<size_t>();
      if (dual.contains("cache_size"))
        config.dual_rocksdb_cache_size = dual["cache_size"].get<size_t>();
      if (dual.contains("hot_ratio"))
        config.dual_rocksdb_hot_ratio = dual["hot_ratio"].get<double>();
      if (dual.contains("medium_ratio"))
        config.dual_rocksdb_medium_ratio = dual["medium_ratio"].get<double>();
      if (dual.contains("dynamic_cache"))
        config.dual_rocksdb_dynamic_cache = dual["dynamic_cache"].get<bool>();
    }

    return config;

  } catch (const json::exception &e) {
    throw ConfigError("JSON parse error in config file: " +
                      std::string(e.what()));
  }
}

void BenchmarkConfig::save_to_file(const std::string &config_path) const {
  json j;

  j["benchmark"] = {{"storage_strategy", storage_strategy},
                    {"db_path", db_path},
                    {"initial_records", initial_records},
                    {"hotspot_updates", hotspot_updates},
                    {"enable_bloom_filter", enable_bloom_filter},
                    {"enable_compression", enable_compression},
                    {"clean_existing_data", clean_existing_data},
                    {"verbose", verbose}};

  j["dual_rocksdb"] = {{"range_size", dual_rocksdb_range_size},
                       {"cache_size", dual_rocksdb_cache_size},
                       {"hot_ratio", dual_rocksdb_hot_ratio},
                       {"medium_ratio", dual_rocksdb_medium_ratio},
                       {"dynamic_cache", dual_rocksdb_dynamic_cache}};

  std::ofstream file(config_path);
  if (!file.is_open()) {
    throw ConfigError("Cannot write to config file: " + config_path);
  }

  file << j.dump(4); // 缩进4个空格，格式化输出
}

void BenchmarkConfig::print_config() const {
  utils::log_info("=== Benchmark Configuration ===");
  utils::log_info("Storage Strategy: {}", storage_strategy);
  utils::log_info("Database Path: {}", db_path);
  utils::log_info("Initial Records: {}", initial_records);
  utils::log_info("Hotspot Updates: {}", hotspot_updates);
  utils::log_info("Bloom Filter: {}",
                  enable_bloom_filter ? "Enabled" : "Disabled");
  utils::log_info("Compression: {}",
                  enable_compression ? "Enabled" : "Disabled");
  utils::log_info("Clean Existing Data: {}",
                  clean_existing_data ? "Yes" : "No");
  utils::log_info("Verbose Output: {}", verbose ? "Yes" : "No");

  if (storage_strategy == "dual_rocksdb_adaptive") {
    utils::log_info("DualRocksDB Config:");
    utils::log_info("  Range Size: {}", dual_rocksdb_range_size);
    utils::log_info("  Cache Size: {} MB",
                    dual_rocksdb_cache_size / (1024 * 1024));
    utils::log_info("  Hot Cache Ratio: {:.2f}%", dual_rocksdb_hot_ratio * 100);
    utils::log_info("  Medium Cache Ratio: {:.2f}%",
                    dual_rocksdb_medium_ratio * 100);
    utils::log_info("  Dynamic Cache: {}", dual_rocksdb_dynamic_cache ? "Enabled" : "Disabled");
    utils::log_info("  Bloom Filters: Always Enabled (Optimized)");
  }

  utils::log_info("==============================");
}

bool BenchmarkConfig::validate() const {
  return get_validation_errors().empty();
}

std::vector<std::string> BenchmarkConfig::get_validation_errors() const {
  std::vector<std::string> errors;

  if (initial_records == 0) {
    errors.push_back("Initial records must be greater than 0");
  }

  if (hotspot_updates > initial_records) {
    errors.push_back("Hotspot updates cannot exceed initial records");
  }

  if (dual_rocksdb_hot_ratio + dual_rocksdb_medium_ratio > 1.0) {
    errors.push_back("Hot + medium cache ratio cannot exceed 1.0");
  }

  if (storage_strategy == "dual_rocksdb_adaptive") {
    if (dual_rocksdb_range_size == 0) {
      errors.push_back("DualRocksDB range size must be greater than 0");
    }
    if (dual_rocksdb_cache_size == 0) {
      errors.push_back("DualRocksDB cache size must be greater than 0");
    }
  }

  return errors;
}

std::string BenchmarkConfig::get_strategy_config() const {
  std::ostringstream oss;

  if (storage_strategy == "dual_rocksdb_adaptive") {
    oss << "range_size=" << dual_rocksdb_range_size
        << ",cache_size=" << dual_rocksdb_cache_size
        << ",hot_ratio=" << dual_rocksdb_hot_ratio
        << ",medium_ratio=" << dual_rocksdb_medium_ratio << ",compression="
        << (enable_compression ? "enabled" : "disabled")
        << ",bloom=always_enabled";
  }

  return oss.str();
}

void BenchmarkConfig::print_help(const std::string &program_name) {
  std::cout << "Usage: " << program_name << " [options] [db_path]\n";
  std::cout << "\nBasic Options:\n";
  std::cout << "  -s,--strategy STRATEGY       Storage strategy "
               "(page_index|direct_version|dual_rocksdb_adaptive)\n";
  std::cout << "  -d,--db-path PATH            Database path (default: "
               "./rocksdb_data)\n";
  std::cout << "  -i,--initial-records N        Number of initial records "
               "(default: 100000000)\n";
  std::cout << "  -u,--hotspot-updates N        Number of hotspot updates "
               "(default: 10000000)\n";
  std::cout
      << "  -c,--clean-data              Clean existing data before starting\n";
  std::cout << "  -v,--verbose                 Enable verbose output\n";
  std::cout << "  --disable-bloom-filter       Disable bloom filter\n";
  std::cout << "  --enable-compression        Enable compression for all strategies\n";
  std::cout << "  -h,--help                    Show this help message\n";
  std::cout << "  --version                    Show version information\n";
  std::cout << "\nDualRocksDB Options:\n";
  std::cout << "  --dual-range-size N          Range size for DualRocksDB "
               "strategy (default: 10000)\n";
  std::cout << "  --dual-cache-size N          Cache size in bytes for "
               "DualRocksDB strategy (default: 1GB)\n";
  std::cout << "  --dual-hot-ratio RATIO       Hot cache ratio for DualRocksDB "
               "strategy (default: 0.01)\n";
  std::cout << "  --dual-medium-ratio RATIO    Medium cache ratio for "
               "DualRocksDB strategy (default: 0.05)\n";
  std::cout << "  --dual-enable-dynamic-cache  Enable dynamic cache optimization "
               "for DualRocksDB strategy\n";
  std::cout << "\nExamples:\n";
  std::cout << "  " << program_name
            << " --strategy dual_rocksdb_adaptive --clean-data\n";
  std::cout << "  " << program_name
            << " -s dual_rocksdb_adaptive -i 1000000 -u 100000 -c\n";
  std::cout << "  " << program_name << " --config benchmark_config.json\n";
}