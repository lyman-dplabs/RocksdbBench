# Strategy Comparison Summary

## Overview

This document provides a quick comparison of the implemented storage strategies and their characteristics.

## Strategy Details

### PageIndexStrategy (Original)

**Data Structure:**
```
ChangeSet Table: {block_num:8x}{address_slot} → value
Index Table:     {page_num:8x}{address_slot} → [block_num_list]
```

**Query Pattern:**
1. Find latest block: Scan pages in Index table
2. Get value: Direct lookup in ChangeSet table

**Characteristics:**
- ✅ Mature and well-tested
- ✅ Efficient merge operations
- ✅ Page-based indexing reduces memory
- ❌ Two-query overhead for value lookups
- ❌ Index scanning can be slow for sparse keys

**Best For:**
- Workloads with good key locality
- Scenarios requiring proven reliability
- Memory-constrained environments

### DirectVersionStrategy (New)

**Data Structure:**
```
Version Index: VERSION|{address_slot}:{version:16x} → block_number  
Data Store:    DATA|{block_num:8x}|{address_slot} → value
```

**Query Pattern:**
1. Build max version key: `VERSION|{key}:FFFFFFFF`
2. Seek to find latest version
3. Get block number, then read value

**Characteristics:**
- ✅ Single seek operation for latest values
- ✅ Key prefix organization enables efficient range queries
- ✅ Separation of versioning from data storage
- ❌ Two-layer storage (version index + data)
- ❌ Key encoding overhead

**Best For:**
- Latest-value queries (common in blockchain state)
- Workloads with frequent version lookups
- Scenarios needing efficient range queries

## Performance Characteristics

### Write Performance
- **PageIndex**: Moderate (merge operations + index updates)
- **DirectVersion**: Faster (simple key-value writes)

### Read Performance (Latest Value)
- **PageIndex**: Slower (index scan + data lookup)
- **DirectVersion**: Faster (single seek operation)

### Read Performance (Historical Query)
- **PageIndex**: Moderate (targeted page scan)
- **DirectVersion**: Moderate (version range seek)

### Storage Overhead
- **PageIndex**: Higher (index maintenance)
- **DirectVersion**: Lower (simpler structure)

### Memory Usage
- **PageIndex**: Moderate (page caching)
- **DirectVersion**: Lower (direct key access)

## Usage Examples

```bash
# Test PageIndexStrategy
./scripts/run.sh --strategy page_index --clean

# Test DirectVersionStrategy  
./scripts/run.sh --strategy direct_version --clean

# Compare both strategies
./scripts/test_both_strategies.sh

# Custom dataset sizes
./scripts/run.sh --strategy direct_version --initial-records 50000000 --hotspot-updates 5000000
```

## When to Use Each Strategy

### Choose PageIndexStrategy when:
- You need proven, battle-tested implementation
- Memory usage is a critical concern
- Your workload has good key locality
- You're doing mostly historical range queries

### Choose DirectVersionStrategy when:
- Latest-value queries are common
- You need efficient version management
- Storage efficiency is important
- You're testing new blockchain storage patterns

## Future Strategy Ideas

Based on the implementation patterns, consider these strategies:

1. **SimpleKeyBlockStrategy**: Direct key-value without complex indexing
2. **TimeSeriesStrategy**: Time-bucketed storage for range queries  
3. **CompressedStrategy**: Value compression for storage efficiency
4. **HybridStrategy**: Combine best aspects of multiple approaches

## Testing Recommendations

1. **Always test with clean data**: Use `--clean` flag
2. **Compare multiple runs**: Strategies may have warm-up effects
3. **Monitor system resources**: Watch CPU, memory, and disk I/O
4. **Test realistic workloads**: Use production-like data patterns
5. **Document findings**: Record performance characteristics for your use case