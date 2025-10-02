# Strategy Comparison: Direct vs Dual RocksDB

## Overview

This document compares the two storage strategies implemented for historical version query testing in RocksDB benchmarking.

## DirectVersion Strategy

### Architecture
- **Single Database**: Uses one RocksDB instance with prefixed keys
- **Key Format**: `VERSION|{address_slot}:{block_number}` → `value`
- **Storage Layout**: All data stored in a single column family with version prefixes
- **Query Method**: Direct seek with prefix matching

### Strengths
- **Simplicity**: Single database, straightforward implementation
- **Low Overhead**: No additional database management complexity
- **Memory Efficient**: No duplicate data or complex caching structures
- **Fast Setup**: Single database initialization

### Weaknesses
- **Limited Optimization**: No range-based optimizations
- **Competition**: Updates and queries compete for the same resources
- **Scalability**: Performance may degrade with very large datasets

## Dual RocksDB Strategy

### Architecture
- **Dual Database**: Two separate RocksDB instances
  - **Range Index DB**: Maps addresses to block ranges
  - **Data Storage DB**: Stores actual data with range prefixes
- **Key Format**: `R{range_num}|{address_slot}|{block_number}` → `value`
- **Range Management**: Blocks grouped into configurable ranges (default: 5000 blocks)
- **Adaptive Caching**: Multi-level cache system (hot/medium/passive)

### Strengths
- **Range Optimization**: Efficient range-based queries and batch operations
- **Load Separation**: Range index and data storage compete for different resources
- **Adaptive Caching**: Intelligent cache management based on access patterns
- **Scalability**: Better performance for large datasets

### Weaknesses
- **Complexity**: More complex implementation with dual database management
- **Memory Overhead**: Additional caching structures and duplicate indexing
- **Setup Cost**: Two database instances to initialize and manage

## Performance Characteristics

### Historical Version Query
- **Direct**: O(log N) seek per query, single database operation
- **Dual**: O(log R) range lookup + O(log N) data query, where R is number of ranges

### Write Performance
- **Direct**: Single write operation per record
- **Dual**: Range index update + data storage write (two operations)

### Memory Usage
- **Direct**: Minimal overhead, primarily RocksDB memory usage
- **Dual**: Additional cache memory for range management (configurable)

## Configuration Parameters

### DirectVersion Strategy
```cpp
batch_size_blocks = 5          // Blocks per write batch
max_batch_size_bytes = 4GB     // Maximum batch size
```

### Dual RocksDB Strategy
```cpp
range_size = 5000              // Blocks per range
cache_size = 128MB             // Total cache memory
hot_cache_ratio = 1%           // Hot data cache percentage
medium_cache_ratio = 5%        // Medium data cache percentage
batch_size_blocks = 5          // Blocks per write batch
max_batch_size_bytes = 4GB     // Maximum batch size
```

## Use Cases

### Choose DirectVersion When:
- Simplicity is preferred over optimization
- Memory constraints are tight
- Dataset size is moderate
- Testing focuses on basic historical version queries

### Choose Dual RocksDB When:
- Maximum performance is required
- Dataset size is large
- Complex query patterns are expected
- Range-based optimizations are beneficial

## Benchmarking Results

The tool supports running historical version query tests with both strategies to compare performance under specific workloads.

### Test Configuration
- Default test: 1000 keys, 360 minutes (6 hours)
- Update-query loop: One block update followed by multiple historical queries
- Performance metrics: Write latency, query latency, cache hit rates

## Summary

Both strategies implement the required historical version query semantics correctly:
- **Query Logic**: ≤target_version find latest, else find ≥minimum
- **Data Integrity**: Consistent results across both implementations
- **Performance Trade-offs**: Simplicity vs. optimization

The choice between strategies depends on specific testing requirements and performance characteristics needed for the benchmark scenario.