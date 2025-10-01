#include "metrics_collector.hpp"
#include "../utils/logger.hpp"

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
    utils::log_info("\n=== RocksDB Benchmark Performance Metrics Summary ===");
    utils::log_info("=== 关键性能指标 (KPIs) ===\n");
    
    utils::log_info("【写入吞吐量 Write Throughput】");
    utils::log_info("  Total keys written: {}", write_metrics_.total_keys_written);
    utils::log_info("  Total bytes written: {} bytes", write_metrics_.total_bytes_written);
    utils::log_info("  Total write time: {:.2f} ms", write_metrics_.total_time_ms);
    utils::log_info("  Write batches: {}", write_metrics_.batch_count);
    
    if (write_metrics_.total_time_ms > 0) {
        double avg_throughput = (write_metrics_.total_bytes_written / (1024.0 * 1024.0)) / 
                               (write_metrics_.total_time_ms / 1000.0);
        utils::log_info("  Average write throughput: {:.2f} MB/s", avg_throughput);
        
        if (write_metrics_.batch_count > 0) {
            double avg_batch_time = write_metrics_.total_time_ms / write_metrics_.batch_count;
            double batch_throughput = (write_metrics_.total_bytes_written / write_metrics_.batch_count / (1024.0 * 1024.0)) / (avg_batch_time / 1000.0);
            utils::log_info("  Per-batch (10,000 keys) write throughput: {:.2f} MB/s", batch_throughput);
        }
    }
    
    utils::log_info("\n【历史查询性能 Historical Query Performance】");
    utils::log_info("  Total queries: {}", query_metrics_.total_queries);
    utils::log_info("  Successful queries: {}", query_metrics_.successful_queries);
    utils::log_info("  Query success rate: {:.2f}%", 
              query_metrics_.total_queries > 0 ? 
              (query_metrics_.successful_queries * 100.0 / query_metrics_.total_queries) : 0.0);
    utils::log_info("  Average query time: {:.3f} ms", query_metrics_.avg_query_time_ms);
    
    if (query_metrics_.successful_queries > 0) {
        double success_rate = query_metrics_.successful_queries * 100.0 / query_metrics_.total_queries;
        if (success_rate >= 90.0) {
            utils::log_info("  ✓ Query success rate meets requirement (≥90%)");
        } else {
            utils::log_info("  ⚠ Query success rate below requirement (≥90%)");
        }
    }
    
    utils::log_info("\n【SST合并效率 SST Compaction Efficiency】");
    utils::log_info("  Total compactions: {}", compaction_metrics_.total_compactions);
    utils::log_info("  Total compaction time: {:.2f} ms", compaction_metrics_.total_compaction_time_ms);
    utils::log_info("  Bytes compacted: {} bytes", compaction_metrics_.bytes_compacted);
    utils::log_info("  Levels compacted: {}", compaction_metrics_.levels_compacted);
    if (compaction_metrics_.total_compactions > 0) {
        double avg_compaction_time = compaction_metrics_.total_compaction_time_ms / compaction_metrics_.total_compactions;
        double compaction_throughput = compaction_metrics_.bytes_compacted / (1024.0 * 1024.0) / (compaction_metrics_.total_compaction_time_ms / 1000.0);
        utils::log_info("  Average compaction time: {:.2f} ms", avg_compaction_time);
        utils::log_info("  Compaction throughput: {:.2f} MB/s", compaction_throughput);
    }
    
    utils::log_info("\n【MergeOperator聚合大小 MergeOperator Aggregation Size】");
    utils::log_info("  Total merge operations: {}", merge_operator_metrics_.total_merges);
    utils::log_info("  Total merged values: {}", merge_operator_metrics_.total_merged_values);
    utils::log_info("  Average merged value size: {:.2f} bytes", merge_operator_metrics_.avg_merged_value_size);
    utils::log_info("  Max merged value size: {} bytes", merge_operator_metrics_.max_merged_value_size);
    if (merge_operator_metrics_.total_merges > 0) {
        double avg_values_per_merge = static_cast<double>(merge_operator_metrics_.total_merged_values) / merge_operator_metrics_.total_merges;
        utils::log_info("  Average values per merge: {:.2f}", avg_values_per_merge);
    }
    
    utils::log_info("\n【Bloom Filter准确率 Bloom Filter Accuracy】");
    utils::log_info("  Total point queries: {}", bloom_filter_metrics_.total_point_queries);
    utils::log_info("  Bloom filter hits: {}", bloom_filter_metrics_.bloom_filter_hits);
    utils::log_info("  Bloom filter misses: {}", bloom_filter_metrics_.bloom_filter_misses);
    utils::log_info("  False positive rate: {:.2f}%", bloom_filter_metrics_.false_positive_rate);
    if (bloom_filter_metrics_.total_point_queries > 0) {
        double hit_rate = static_cast<double>(bloom_filter_metrics_.bloom_filter_hits) / bloom_filter_metrics_.total_point_queries * 100.0;
        utils::log_info("  Bloom filter hit rate: {:.2f}%", hit_rate);
    }
    
    utils::log_info("\n【冷热Key命中分析 Hot/Cold Key Hit Analysis】");
    utils::log_info("  Hot key queries: {}", cache_hit_metrics_.hot_key_queries);
    utils::log_info("  Hot key hits: {}", cache_hit_metrics_.hot_key_hits);
    if (cache_hit_metrics_.hot_key_queries > 0) {
        double hot_hit_rate = static_cast<double>(cache_hit_metrics_.hot_key_hits) / cache_hit_metrics_.hot_key_queries * 100.0;
        utils::log_info("  Hot key hit rate: {:.2f}%", hot_hit_rate);
    }
    
    utils::log_info("  Medium key queries: {}", cache_hit_metrics_.medium_key_queries);
    utils::log_info("  Medium key hits: {}", cache_hit_metrics_.medium_key_hits);
    if (cache_hit_metrics_.medium_key_queries > 0) {
        double medium_hit_rate = static_cast<double>(cache_hit_metrics_.medium_key_hits) / cache_hit_metrics_.medium_key_queries * 100.0;
        utils::log_info("  Medium key hit rate: {:.2f}%", medium_hit_rate);
    }
    
    utils::log_info("  Tail key queries: {}", cache_hit_metrics_.tail_key_queries);
    utils::log_info("  Tail key hits: {}", cache_hit_metrics_.tail_key_hits);
    if (cache_hit_metrics_.tail_key_queries > 0) {
        double tail_hit_rate = static_cast<double>(cache_hit_metrics_.tail_key_hits) / cache_hit_metrics_.tail_key_queries * 100.0;
        utils::log_info("  Tail key hit rate: {:.2f}%", tail_hit_rate);
    }
    
    utils::log_info("\n=== Summary ===");
    if (write_metrics_.total_time_ms > 0) {
        double total_gb = write_metrics_.total_bytes_written / (1024.0 * 1024.0 * 1024.0);
        double total_seconds = write_metrics_.total_time_ms / 1000.0;
        double overall_throughput = total_gb / total_seconds;
        
        utils::log_info("  Overall performance: {:.2f} GB/s total write throughput", overall_throughput);
    }
    
    utils::log_info("  Query performance: {:.3f} ms average query latency", query_metrics_.avg_query_time_ms);
    
    utils::log_info("=================================== ===");
}