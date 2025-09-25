#include "metrics_collector.hpp"
#include <fmt/format.h>

MetricsCollector::MetricsCollector() = default;

void MetricsCollector::start_write_timer() {
    if (write_timer_running_) return;
    write_start_time_ = std::chrono::high_resolution_clock::now();
    write_timer_running_ = true;
}

void MetricsCollector::stop_and_record_write(size_t keys_written, size_t bytes_written) {
    if (!write_timer_running_) return;
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - write_start_time_);
    double time_ms = duration.count();
    
    write_metrics_.total_keys_written += keys_written;
    write_metrics_.total_bytes_written += bytes_written;
    write_metrics_.total_time_ms += time_ms;
    write_metrics_.batch_count++;
    
    if (time_ms > 0) {
        double throughput_mbps = (bytes_written / (1024.0 * 1024.0)) / (time_ms / 1000.0);
        write_metrics_.avg_throughput_mbps = 
            (write_metrics_.avg_throughput_mbps * (write_metrics_.batch_count - 1) + throughput_mbps) / 
            write_metrics_.batch_count;
    }
    
    write_timer_running_ = false;
}

void MetricsCollector::start_query_timer() {
    if (query_timer_running_) return;
    query_start_time_ = std::chrono::high_resolution_clock::now();
    query_timer_running_ = true;
}

void MetricsCollector::stop_and_record_query(bool success) {
    if (!query_timer_running_) return;
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - query_start_time_);
    double time_ms = duration.count() / 1000.0;
    
    query_metrics_.total_queries++;
    if (success) {
        query_metrics_.successful_queries++;
    }
    query_metrics_.total_query_time_ms += time_ms;
    query_metrics_.avg_query_time_ms = query_metrics_.total_query_time_ms / query_metrics_.total_queries;
    
    query_timer_running_ = false;
}

void MetricsCollector::record_compaction(double time_ms, size_t bytes_compacted, size_t levels_compacted) {
    compaction_metrics_.total_compactions++;
    compaction_metrics_.total_compaction_time_ms += time_ms;
    compaction_metrics_.bytes_compacted += bytes_compacted;
    compaction_metrics_.levels_compacted += levels_compacted;
}

void MetricsCollector::record_merge_operation(size_t merged_values, size_t merged_value_size) {
    merge_operator_metrics_.total_merges++;
    merge_operator_metrics_.total_merged_values += merged_values;
    
    // Update average merged value size
    if (merge_operator_metrics_.total_merges > 0) {
        merge_operator_metrics_.avg_merged_value_size = 
            (merge_operator_metrics_.avg_merged_value_size * (merge_operator_metrics_.total_merges - 1) + merged_value_size) / 
            merge_operator_metrics_.total_merges;
    }
    
    // Update max merged value size
    if (merged_value_size > merge_operator_metrics_.max_merged_value_size) {
        merge_operator_metrics_.max_merged_value_size = merged_value_size;
    }
}

void MetricsCollector::record_bloom_filter_query(bool hit) {
    bloom_filter_metrics_.total_point_queries++;
    if (hit) {
        bloom_filter_metrics_.bloom_filter_hits++;
    } else {
        bloom_filter_metrics_.bloom_filter_misses++;
    }
    
    // Calculate false positive rate
    if (bloom_filter_metrics_.total_point_queries > 0) {
        bloom_filter_metrics_.false_positive_rate = 
            static_cast<double>(bloom_filter_metrics_.bloom_filter_misses) / 
            bloom_filter_metrics_.total_point_queries * 100.0;
    }
}

void MetricsCollector::record_cache_hit(const std::string& key_type, bool hit) {
    if (key_type == "hot") {
        cache_hit_metrics_.hot_key_queries++;
        if (hit) cache_hit_metrics_.hot_key_hits++;
    } else if (key_type == "medium") {
        cache_hit_metrics_.medium_key_queries++;
        if (hit) cache_hit_metrics_.medium_key_hits++;
    } else if (key_type == "tail") {
        cache_hit_metrics_.tail_key_queries++;
        if (hit) cache_hit_metrics_.tail_key_hits++;
    }
}

void MetricsCollector::report_summary() const {
    fmt::print("\n=== RocksDB Benchmark Performance Metrics Summary ===\n");
    fmt::print("=== 关键性能指标 (KPIs) ===\n\n");
    
    fmt::print("【写入吞吐量 Write Throughput】\n");
    fmt::print("  Total keys written: {}\n", write_metrics_.total_keys_written);
    fmt::print("  Total bytes written: {} bytes\n", write_metrics_.total_bytes_written);
    fmt::print("  Total write time: {:.2f} ms\n", write_metrics_.total_time_ms);
    fmt::print("  Write batches: {}\n", write_metrics_.batch_count);
    
    if (write_metrics_.total_time_ms > 0) {
        double avg_throughput = (write_metrics_.total_bytes_written / (1024.0 * 1024.0)) / 
                               (write_metrics_.total_time_ms / 1000.0);
        fmt::print("  Average write throughput: {:.2f} MB/s\n", avg_throughput);
        
        if (write_metrics_.batch_count > 0) {
            double avg_batch_time = write_metrics_.total_time_ms / write_metrics_.batch_count;
            double batch_throughput = (write_metrics_.total_bytes_written / write_metrics_.batch_count / (1024.0 * 1024.0)) / (avg_batch_time / 1000.0);
            fmt::print("  Per-batch (10,000 keys) write throughput: {:.2f} MB/s\n", batch_throughput);
        }
    }
    
    fmt::print("\n【历史查询性能 Historical Query Performance】\n");
    fmt::print("  Total queries: {}\n", query_metrics_.total_queries);
    fmt::print("  Successful queries: {}\n", query_metrics_.successful_queries);
    fmt::print("  Query success rate: {:.2f}%\n", 
              query_metrics_.total_queries > 0 ? 
              (query_metrics_.successful_queries * 100.0 / query_metrics_.total_queries) : 0.0);
    fmt::print("  Average query time: {:.3f} ms\n", query_metrics_.avg_query_time_ms);
    
    if (query_metrics_.successful_queries > 0) {
        double success_rate = query_metrics_.successful_queries * 100.0 / query_metrics_.total_queries;
        if (success_rate >= 90.0) {
            fmt::print("  ✓ Query success rate meets requirement (≥90%)\n");
        } else {
            fmt::print("  ⚠ Query success rate below requirement (≥90%)\n");
        }
    }
    
    fmt::print("\n【SST合并效率 SST Compaction Efficiency】\n");
    fmt::print("  Total compactions: {}\n", compaction_metrics_.total_compactions);
    fmt::print("  Total compaction time: {:.2f} ms\n", compaction_metrics_.total_compaction_time_ms);
    fmt::print("  Bytes compacted: {} bytes\n", compaction_metrics_.bytes_compacted);
    fmt::print("  Levels compacted: {}\n", compaction_metrics_.levels_compacted);
    if (compaction_metrics_.total_compactions > 0) {
        double avg_compaction_time = compaction_metrics_.total_compaction_time_ms / compaction_metrics_.total_compactions;
        double compaction_throughput = compaction_metrics_.bytes_compacted / (1024.0 * 1024.0) / (compaction_metrics_.total_compaction_time_ms / 1000.0);
        fmt::print("  Average compaction time: {:.2f} ms\n", avg_compaction_time);
        fmt::print("  Compaction throughput: {:.2f} MB/s\n", compaction_throughput);
    }
    
    fmt::print("\n【MergeOperator聚合大小 MergeOperator Aggregation Size】\n");
    fmt::print("  Total merge operations: {}\n", merge_operator_metrics_.total_merges);
    fmt::print("  Total merged values: {}\n", merge_operator_metrics_.total_merged_values);
    fmt::print("  Average merged value size: {:.2f} bytes\n", merge_operator_metrics_.avg_merged_value_size);
    fmt::print("  Max merged value size: {} bytes\n", merge_operator_metrics_.max_merged_value_size);
    if (merge_operator_metrics_.total_merges > 0) {
        double avg_values_per_merge = static_cast<double>(merge_operator_metrics_.total_merged_values) / merge_operator_metrics_.total_merges;
        fmt::print("  Average values per merge: {:.2f}\n", avg_values_per_merge);
    }
    
    fmt::print("\n【Bloom Filter准确率 Bloom Filter Accuracy】\n");
    fmt::print("  Total point queries: {}\n", bloom_filter_metrics_.total_point_queries);
    fmt::print("  Bloom filter hits: {}\n", bloom_filter_metrics_.bloom_filter_hits);
    fmt::print("  Bloom filter misses: {}\n", bloom_filter_metrics_.bloom_filter_misses);
    fmt::print("  False positive rate: {:.2f}%\n", bloom_filter_metrics_.false_positive_rate);
    if (bloom_filter_metrics_.total_point_queries > 0) {
        double hit_rate = static_cast<double>(bloom_filter_metrics_.bloom_filter_hits) / bloom_filter_metrics_.total_point_queries * 100.0;
        fmt::print("  Bloom filter hit rate: {:.2f}%\n", hit_rate);
    }
    
    fmt::print("\n【冷热Key命中分析 Hot/Cold Key Hit Analysis】\n");
    fmt::print("  Hot key queries: {}\n", cache_hit_metrics_.hot_key_queries);
    fmt::print("  Hot key hits: {}\n", cache_hit_metrics_.hot_key_hits);
    if (cache_hit_metrics_.hot_key_queries > 0) {
        double hot_hit_rate = static_cast<double>(cache_hit_metrics_.hot_key_hits) / cache_hit_metrics_.hot_key_queries * 100.0;
        fmt::print("  Hot key hit rate: {:.2f}%\n", hot_hit_rate);
    }
    
    fmt::print("  Medium key queries: {}\n", cache_hit_metrics_.medium_key_queries);
    fmt::print("  Medium key hits: {}\n", cache_hit_metrics_.medium_key_hits);
    if (cache_hit_metrics_.medium_key_queries > 0) {
        double medium_hit_rate = static_cast<double>(cache_hit_metrics_.medium_key_hits) / cache_hit_metrics_.medium_key_queries * 100.0;
        fmt::print("  Medium key hit rate: {:.2f}%\n", medium_hit_rate);
    }
    
    fmt::print("  Tail key queries: {}\n", cache_hit_metrics_.tail_key_queries);
    fmt::print("  Tail key hits: {}\n", cache_hit_metrics_.tail_key_hits);
    if (cache_hit_metrics_.tail_key_queries > 0) {
        double tail_hit_rate = static_cast<double>(cache_hit_metrics_.tail_key_hits) / cache_hit_metrics_.tail_key_queries * 100.0;
        fmt::print("  Tail key hit rate: {:.2f}%\n", tail_hit_rate);
    }
    
    fmt::print("\n=== Summary ===\n");
    if (write_metrics_.total_time_ms > 0) {
        double total_gb = write_metrics_.total_bytes_written / (1024.0 * 1024.0 * 1024.0);
        double total_seconds = write_metrics_.total_time_ms / 1000.0;
        double overall_throughput = total_gb / total_seconds;
        
        fmt::print("  Overall performance: {:.2f} GB/s total write throughput\n", overall_throughput);
    }
    
    fmt::print("  Query performance: {:.3f} ms average query latency\n", query_metrics_.avg_query_time_ms);
    
    fmt::print("===================================\n");
}