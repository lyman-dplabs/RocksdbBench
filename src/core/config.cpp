#include "config.hpp"
#include "../utils/logger.hpp"
#include <CLI/CLI.hpp>
#include <cctype>
#include <iostream>

// BenchmarkConfig实现 - 简化版本，专注于test.mdx需求
BenchmarkConfig BenchmarkConfig::from_args(int argc, char *argv[]) {
  BenchmarkConfig config;

  CLI::App app{"RocksDB Historical Version Query Test Tool"};

  // 基本选项
  app.add_option("-s,--strategy", config.storage_strategy,
                 "Storage strategy to use (direct_version, dual_rocksdb_adaptive)")
      ->check(CLI::IsMember({"direct_version", "dual_rocksdb_adaptive"}))
      ->default_val("direct_version");

  app.add_option("-d,--db-path", config.db_path, "Database path")
      ->default_val("./rocksdb_data");

  app.add_option("-k,--total-keys", config.total_keys,
                 "Total number of keys for testing")
      ->default_val(1000)
      ->check(CLI::PositiveNumber);

  app.add_option("-t,--duration", config.continuous_duration_minutes,
                 "Test duration in minutes (default: 360 minutes = 6 hours)")
      ->default_val(360)
      ->check(CLI::PositiveNumber);

  // 布尔选项
  app.add_flag("--disable-bloom-filter", config.enable_bloom_filter,
               "Disable bloom filter (default: enabled)")
      ->default_val(true);

  app.add_flag("-c,--clean-data", config.clean_existing_data,
               "Clean existing data before starting");

  app.add_flag("-v,--verbose", config.verbose, "Enable verbose output");

  // 策略特定选项
  app.add_option("--range-size", config.range_size,
                 "Range size for dual_rocksdb_adaptive strategy")
      ->default_val(5000)
      ->check(CLI::PositiveNumber);

  app.add_option("--cache-size", config.cache_size,
                 "Cache size in bytes")
      ->default_val(128 * 1024 * 1024)
      ->check(CLI::PositiveNumber);

  // Batch配置选项
  app.add_option("--batch-size-blocks", config.batch_size_blocks,
                 "Number of blocks per write batch (default: 5)")
      ->default_val(5)
      ->check(CLI::PositiveNumber);

  app.add_option("--max-batch-size-bytes", config.max_batch_size_bytes,
                 "Maximum batch size in bytes (default: 4GB)")
      ->default_val(4UL * 1024 * 1024 * 1024)
      ->check(CLI::PositiveNumber);

  // 位置参数
  app.add_option("db_path_pos", config.db_path,
                 "Database path (positional argument)")
      ->expected(0, 1);

  // 设置帮助信息格式
  app.set_help_flag("-h,--help", "Show help message");
  
  // 版本选项
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
    app.parse(argc, argv);

    // 检查版本标志
    if (version_flag) {
      print_version_info();
      std::exit(0);
    }

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

void BenchmarkConfig::print_config() const {
  utils::log_info("=== Historical Version Query Test Configuration ===");
  utils::log_info("Storage Strategy: {}", storage_strategy);
  utils::log_info("Database Path: {}", db_path);
  utils::log_info("Total Keys: {}", total_keys);
  utils::log_info("Test Duration: {} minutes", continuous_duration_minutes);
  utils::log_info("Bloom Filter: {}", enable_bloom_filter ? "Enabled" : "Disabled");
  utils::log_info("Clean Existing Data: {}", clean_existing_data ? "Yes" : "No");
  utils::log_info("Verbose Output: {}", verbose ? "Yes" : "No");
  utils::log_info("Batch Size Blocks: {}", batch_size_blocks);
  utils::log_info("Max Batch Size: {} MB", max_batch_size_bytes / (1024 * 1024));

  if (storage_strategy == "dual_rocksdb_adaptive") {
    utils::log_info("Range Size: {}", range_size);
    utils::log_info("Cache Size: {} MB", cache_size / (1024 * 1024));
  }

  utils::log_info("================================================");
}

bool BenchmarkConfig::validate() const {
  return get_validation_errors().empty();
}

std::vector<std::string> BenchmarkConfig::get_validation_errors() const {
  std::vector<std::string> errors;

  if (total_keys == 0) {
    errors.push_back("Total keys must be greater than 0");
  }

  if (continuous_duration_minutes == 0) {
    errors.push_back("Duration must be greater than 0");
  }

  if (storage_strategy == "dual_rocksdb_adaptive") {
    if (range_size == 0) {
      errors.push_back("Range size must be greater than 0");
    }
    if (cache_size == 0) {
      errors.push_back("Cache size must be greater than 0");
    }
  }

  return errors;
}

void BenchmarkConfig::print_help(const std::string &program_name) {
  std::cout << "Usage: " << program_name << " [options] [db_path]\n";
  std::cout << "\nBasic Options:\n";
  std::cout << "  -s,--strategy STRATEGY       Storage strategy "
               "(direct_version|dual_rocksdb_adaptive)\n";
  std::cout << "  -d,--db-path PATH            Database path (default: "
               "./rocksdb_data)\n";
  std::cout << "  -k,--total-keys N            Total number of keys for testing "
               "(default: 1000)\n";
  std::cout << "  -t,--duration N              Test duration in minutes "
               "(default: 360 minutes = 6 hours)\n";
  std::cout
      << "  -c,--clean-data              Clean existing data before starting\n";
  std::cout << "  -v,--verbose                 Enable verbose output\n";
  std::cout << "  --disable-bloom-filter       Disable bloom filter\n";
  std::cout << "  -h,--help                    Show this help message\n";
  std::cout << "  --version                    Show version information\n";
  std::cout << "\nStrategy Options:\n";
  std::cout << "  --range-size N               Range size for dual_rocksdb_adaptive "
               "strategy (default: 5000)\n";
  std::cout << "  --cache-size N               Cache size in bytes "
               "(default: 128MB)\n";
  std::cout << "  --batch-size-blocks N       Number of blocks per write batch "
               "(default: 5)\n";
  std::cout << "  --max-batch-size-bytes N    Maximum batch size in bytes "
               "(default: 4GB)\n";
  std::cout << "\nExamples:\n";
  std::cout << "  " << program_name
            << " --strategy direct_version --total-keys 1000 --duration 60\n";
  std::cout << "  " << program_name
            << " -s dual_rocksdb_adaptive -k 5000 -t 120 -c\n";
}