# RocksDB Benchmark Scripts

This directory contains scripts for running and testing the RocksDB benchmark with different storage strategies.

## Scripts Overview

### `run.sh` - Main Benchmark Runner
The primary script for running benchmarks with configurable options.

**Usage:**
```bash
./scripts/run.sh [OPTIONS]
```

**Options:**
- `--db-path PATH` - Database path (default: ./rocksdb_data)
- `--strategy STRATEGY` - Storage strategy: `page_index`, `direct_version`, or `dual_rocksdb_adaptive` (default: page_index)
- `--clean` - Clean existing data before starting
- `--initial-records N` - Number of initial records
- `--hotspot-updates N` - Number of hotspot updates
- `--help, -h` - Show help message

**Examples:**
```bash
# Default: PageIndexStrategy with standard settings
./scripts/run.sh

# Use DirectVersionStrategy
./scripts/run.sh --strategy direct_version

# Use DualRocksDB Adaptive Strategy (NEW)
./scripts/run.sh --strategy dual_rocksdb_adaptive

# Custom dataset size
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 50000000 --hotspot-updates 5000000

# Clean run with custom path
./scripts/run.sh --strategy dual_rocksdb_adaptive --db-path /tmp/my_benchmark --clean
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
- **Mandatory Seek-Last Optimization**: Core lookup mechanism for maximum query performance
- **Range-Based Partitioning**: Configurable range size (default: 10,000 blocks) for scalable data organization
- **Memory Pressure Management**: Automatic monitoring and cache eviction based on system resources
- **Configuration Options**: Customizable cache ratios, compression, bloom filters, and memory limits
- **Large Dataset Optimized**: Designed to efficiently handle 20B+ key-value pairs with intelligent memory management

## Common Workflows

### 1. Quick Test
```bash
./scripts/test_both_strategies.sh
```

### 2. Performance Comparison
```bash
# Run all strategies with identical parameters
./scripts/run.sh --strategy page_index --db-path ./data_page_index --clean --initial-records 100000000
./scripts/run.sh --strategy direct_version --db-path ./data_direct_version --clean --initial-records 100000000
./scripts/run.sh --strategy dual_rocksdb_adaptive --db-path ./data_dual_adaptive --clean --initial-records 100000000
```

### 3. Custom Benchmark
```bash
# DualRocksDB Adaptive with large dataset
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 50000000 --hotspot-updates 10000000 --clean

# Small dataset testing
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 1000 --hotspot-updates 100 --clean
```

### 4. Development and Testing
```bash
# Build
./scripts/build.sh

# Quick test
./scripts/test_both_strategies.sh

# Test new DualRocksDB strategy
./scripts/run.sh --strategy dual_rocksdb_adaptive --initial-records 10000 --hotspot-updates 1000 --clean

# Full benchmark
./scripts/run.sh --strategy dual_rocksdb_adaptive --clean
```

## Output Locations

- **Default database path:** `./rocksdb_data/`
- **Executable:** `./build/rocksdb_bench_app`
- **Logs:** Printed to console

## Tips

1. **First Run:** Use `./scripts/test_both_strategies.sh` to verify everything works
2. **Performance Testing:** Run both strategies with identical parameters for fair comparison
3. **Disk Space:** Large benchmarks (100M+ records) can use significant disk space
4. **Clean Runs:** Use `--clean` to start fresh, especially when switching strategies
5. **Database Paths:** Use different paths for different strategy runs to avoid conflicts