# RocksDB Benchmark Scripts

This directory contains scripts for running and testing the RocksDB benchmark with different storage strategies, featuring a modern CLI11 command line interface.

## Scripts Overview

### `run.sh` - Main Benchmark Runner
The primary script for running benchmarks with configurable options. Features modern CLI11 interface with JSON configuration support.

**Usage:**
```bash
./scripts/run.sh [OPTIONS]
```

**Modern CLI11 Options:**
- `--db-path PATH` - Database path (default: ./rocksdb_data)
- `--strategy STRATEGY` - Storage strategy: `page_index`, `direct_version`, `dual_rocksdb_adaptive`, `simple_keyblock`, `reduced_keyblock` (default: page_index)
- `--clean` - Clean existing data before starting
- `--initial-records N` - Number of initial records (default: 100000000)
- `--hotspot-updates N` - Number of hotspot updates (default: 10000000)
- `--config FILE` - JSON configuration file path
- `--dual-batch-size N` - DualRocksDB batch block count (default: 5, use 1 to disable batching)
- `--dual-max-batch-bytes N` - DualRocksDB max batch size in bytes (default: 128MB)
- `--help, -h` - Show detailed help message
- `--version, -v` - Show version information

**Examples:**
```bash
# Default: PageIndexStrategy with standard settings
./scripts/run.sh

# Use DirectVersionStrategy
./scripts/run.sh --strategy direct_version

# Use DualRocksDB Adaptive Strategy (NEW) with smart batch writing enabled by default
./scripts/run.sh --strategy dual_rocksdb_adaptive

# Custom dataset size with default batch settings
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 50000000 --hotspot-updates 5000000

# Custom batch writing configuration for higher throughput
./scripts/run.sh --strategy dual_rocksdb_adaptive --dual-batch-size 10 --dual-max-batch-bytes 256M

# Disable batch writing for real-time consistency testing
./scripts/run.sh --strategy dual_rocksdb_adaptive --dual-disable-batching

# Clean run with custom path
./scripts/run.sh --strategy dual_rocksdb_adaptive --db-path /tmp/my_benchmark --clean

# JSON configuration file
./scripts/run.sh --config config.json --clean

# Command line arguments override config file
./scripts/run.sh --config config.json --initial-records 5000000 --strategy dual_rocksdb_adaptive --dual-batch-size 8

# Get help
./scripts/run.sh --help

# Get version
./scripts/run.sh --version
```

### `test_both_strategies.sh` - Quick Comparison Test
Runs quick tests with both strategies to verify functionality.

**Usage:**
```bash
./scripts/test_both_strategies.sh
```

This script:
- Cleans existing data
- Runs PageIndexStrategy with small dataset (10K records, 1K updates)
- Runs DirectVersionStrategy with small dataset (10K records, 1K updates)
- Provides confirmation that both strategies work

### `examples.sh` - Interactive Examples
Shows various usage patterns with explanations.

**Usage:**
```bash
./scripts/examples.sh
```

### `build.sh` - Build Script
Compiles the benchmark application.

**Usage:**
```bash
./scripts/build.sh
```

## JSON Configuration Support

The modern CLI11 interface supports JSON configuration files for complex test scenarios:

### JSON Configuration Format

```json
{
  "database_path": "./rocksdb_data",
  "storage_strategy": "dual_rocksdb_adaptive",
  "clean_start": true,
  "initial_records": 100000000,
  "hotspot_updates": 10000000,
  "strategy_config": {
    "dual_rocksdb_adaptive": {
      "enable_dynamic_cache": true,
      "enable_sharding": false,
      "l1_cache_size_mb": 1024,
      "l2_cache_size_mb": 2048,
      "l3_cache_size_mb": 4096
    }
  }
}
```

### Generating Example Configuration

```bash
# Generate example configuration file
./build/generate_config_example > config.json

# Edit the configuration
vim config.json

# Run with configuration
./scripts/run.sh --config config.json --clean
```

### Configuration Priority

1. **Command line arguments** (highest priority)
2. **JSON configuration file** 
3. **Default values** (lowest priority)

This allows you to:
- Set common parameters in JSON config
- Override specific values via command line
- Maintain multiple configuration files for different scenarios

## Storage Strategies

### PageIndexStrategy (Default)
- Traditional ChangeSet + Index tables
- Page-based organization
- Uses merge operators for index updates
- Backward compatible with original implementation

### DirectVersionStrategy
- Two-layer storage approach:
  1. Version index: `VERSION|address_slot:version → block_number`
  2. Data store: `DATA|block_number|address_slot → value`
- Uses seek_last approach for version queries
- Key prefix-based organization (simplified from column families)

### DualRocksDB Adaptive Strategy (NEW)
- **Dual Database Architecture**: Two separate RocksDB instances for optimal performance
  1. **Range Index Database**: Stores address-to-range mappings for efficient lookups
  2. **Data Storage Database**: Stores range-prefixed actual data with optimized organization
- **Adaptive Caching System**: Three-level intelligent caching:
  - L1 Hot Cache: Complete data caching for high-frequency access
  - L2 Medium Cache: Range list caching for balanced memory usage
  - L3 Passive Cache: Query-time caching for memory efficiency
- **Smart Batch Writing**: Intelligent batch writing system (default enabled):
  - **Data Preparation Phase**: Accumulates multiple blocks before writing to RocksDB for 30-50% performance boost
  - **Hotspot Update Phase**: Switches to direct writing for real-time consistency
  - **Dynamic Mode Switching**: Automatically adjusts writing strategy based on current phase
  - **Thread-Safe Operations**: Mutex-protected batch management for concurrent safety
  - **Configurable Parameters**: Customizable batch size (default: 5 blocks) and memory limits (default: 128MB)
- **Mandatory Seek-Last Optimization**: Core lookup mechanism for maximum query performance
- **Range-Based Partitioning**: Configurable range size (default: 10,000 blocks) for scalable data organization
- **Memory Pressure Management**: Automatic monitoring and cache eviction based on system resources
- **Configuration Options**: Customizable cache ratios, compression, bloom filters, and memory limits
- **Large Dataset Optimized**: Designed to efficiently handle 20B+ key-value pairs with intelligent memory management

**Batch Writing Configuration Examples**:
```bash
# Default batch settings (recommended)
./scripts/run.sh --strategy dual_rocksdb_adaptive  # Batch size: 5, Max: 128MB

# High throughput configuration
./scripts/run.sh --strategy dual_rocksdb_adaptive --dual-batch-size 10 --dual-max-batch-bytes 256M

# Disable batch writing for real-time testing
./scripts/run.sh --strategy dual_rocksdb_adaptive --dual-disable-batching

# Memory-constrained configuration
./scripts/run.sh --strategy dual_rocksdb_adaptive --dual-batch-size 3 --dual-max-batch-bytes 64M
```

### SimpleKeyblock Strategy
- Simplified key-value storage implementation
- Basic keyblock organization for fundamental testing
- Minimal overhead and straightforward access patterns
- Ideal for baseline performance comparisons

### ReducedKeyblock Strategy
- Optimized keyblock storage with reduced memory footprint
- Enhanced compression and efficient key organization
- Balanced performance and memory usage
- Suitable for memory-constrained environments

## Common Workflows

### 1. Quick Test
```bash
./scripts/test_both_strategies.sh
```

### 2. Performance Comparison with Modern CLI
```bash
# Run all strategies with identical parameters
./scripts/run.sh --strategy page_index --db-path ./data_page_index --clean --initial-records 100000000
./scripts/run.sh --strategy direct_version --db-path ./data_direct_version --clean --initial-records 100000000
./scripts/run.sh --strategy dual_rocksdb_adaptive --db-path ./data_dual_adaptive --clean --initial-records 100000000
./scripts/run.sh --strategy simple_keyblock --db-path ./data_simple --clean --initial-records 100000000
./scripts/run.sh --strategy reduced_keyblock --db-path ./data_reduced --clean --initial-records 100000000
```

### 3. JSON Configuration Workflows
```bash
# Generate and use JSON configuration
./build/generate_config_example > dual_rocksdb_config.json
vim dual_rocksdb_config.json  # Edit configuration
./scripts/run.sh --config dual_rocksdb_config.json --clean

# Override config file with command line arguments
./scripts/run.sh --config dual_rocksdb_config.json --initial-records 5000000 --db-path ./custom_data
```

### 4. Custom Benchmark Scenarios
```bash
# DualRocksDB Adaptive with large dataset
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 50000000 --hotspot-updates 10000000 --clean

# Small dataset testing for development
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 1000 --hotspot-updates 100 --clean

# Memory-constrained testing
./scripts/run.sh --strategy reduced_keyblock --initial-records 10000000 --hotspot-updates 1000000 --clean

# Baseline performance testing
./scripts/run.sh --strategy simple_keyblock --initial-records 5000000 --hotspot-updates 500000 --clean
```

### 5. Development and Testing Workflow
```bash
# Build the project
./scripts/build.sh

# Quick functionality test
./scripts/test_both_strategies.sh

# Test new DualRocksDB strategy with small dataset
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 10000 --hotspot-updates 1000 --clean

# Test with JSON configuration
./scripts/run.sh --config config.json --clean

# Full benchmark with all strategies
for strategy in page_index direct_version dual_rocksdb_adaptive simple_keyblock reduced_keyblock; do
    ./scripts/run.sh --strategy $strategy --db-path ./data_$strategy --clean --initial-records 1000000
done
```

### 6. CLI11 Help and Information
```bash
# Get detailed help information
./scripts/run.sh --help

# Check version
./scripts/run.sh --version

# Validate configuration file
./build/rocksdb_bench_app --config config.json --help
```

## Output Locations

- **Default database path:** `./rocksdb_data/`
- **Main executable:** `./build/rocksdb_bench_app` (note: moved from `./build/src/`)
- **Config generator:** `./build/generate_config_example`
- **Test executables:** `./build/test_*` (individual unit tests)
- **Logs:** Printed to console
- **JSON configs:** User-specified paths (e.g., `config.json`)

## Modern CLI11 Features

### Advanced Command Line Features

The CLI11 integration provides several modern command line interface features:

1. **Automatic Help Generation**
   ```bash
   ./scripts/run.sh --help
   # Shows detailed help with all options, descriptions, and defaults
   ```

2. **Version Information**
   ```bash
   ./scripts/run.sh --version
   # Shows application version and build information
   ```

3. **Configuration File Validation**
   ```bash
   # JSON configuration files are automatically validated
   ./scripts/run.sh --config invalid_config.json
   # Error: Invalid JSON format or missing required fields
   ```

4. **Type Checking and Validation**
   ```bash
   # Numbers are validated automatically
   ./scripts/run.sh --initial-records not_a_number
   # Error: Invalid number format
   ```

5. **Choice Validation**
   ```bash
   # Strategy names are validated against available options
   ./scripts/run.sh --strategy invalid_strategy
   # Error: Invalid storage strategy choice
   ```

## Tips

1. **First Run:** Use `./scripts/test_both_strategies.sh` to verify everything works
2. **Performance Testing:** Run all strategies with identical parameters for fair comparison
3. **JSON Configuration:** Use JSON files for complex configurations and maintain multiple test scenarios
4. **Command Line Override:** Combine JSON configs with command line arguments for flexibility
5. **Disk Space:** Large benchmarks (100M+ records) can use significant disk space
6. **Clean Runs:** Use `--clean` to start fresh, especially when switching strategies
7. **Database Paths:** Use different paths for different strategy runs to avoid conflicts
8. **Help System:** Use `--help` frequently to explore available options and validate configurations
9. **Development:** Use small datasets (`--initial-records 1000`) for quick development cycles
10. **Configuration Management:** Create specific JSON configs for different test scenarios (development, performance, memory testing)