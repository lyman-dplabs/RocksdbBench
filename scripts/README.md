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
- `--strategy STRATEGY` - Storage strategy: `page_index` or `direct_version` (default: page_index)
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

# Custom dataset size
./scripts/run.sh --strategy direct_version --initial-records 50000000 --hotspot-updates 5000000

# Clean run with custom path
./scripts/run.sh --strategy page_index --db-path /tmp/my_benchmark --clean
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

## Common Workflows

### 1. Quick Test
```bash
./scripts/test_both_strategies.sh
```

### 2. Performance Comparison
```bash
# Run both strategies with identical parameters
./scripts/run.sh --strategy page_index --db-path ./data_page_index --clean --initial-records 100000000
./scripts/run.sh --strategy direct_version --db-path ./data_direct_version --clean --initial-records 100000000
```

### 3. Custom Benchmark
```bash
./scripts/run.sh --strategy direct_version --initial-records 50000000 --hotspot-updates 10000000 --clean
```

### 4. Development and Testing
```bash
# Build
./scripts/build.sh

# Quick test
./scripts/test_both_strategies.sh

# Full benchmark
./scripts/run.sh --strategy direct_version --clean
```

## Output Locations

- **Default database path:** `./rocksdb_data/`
- **Executable:** `./build/src/rocksdb_bench_app`
- **Logs:** Printed to console

## Tips

1. **First Run:** Use `./scripts/test_both_strategies.sh` to verify everything works
2. **Performance Testing:** Run both strategies with identical parameters for fair comparison
3. **Disk Space:** Large benchmarks (100M+ records) can use significant disk space
4. **Clean Runs:** Use `--clean` to start fresh, especially when switching strategies
5. **Database Paths:** Use different paths for different strategy runs to avoid conflicts