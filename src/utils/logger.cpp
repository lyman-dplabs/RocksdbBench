#include "logger.hpp"
#include <rocksdb/statistics.h>

namespace utils {

void print_compaction_statistics(const std::string& db_name, rocksdb::Statistics* statistics) {
    if (!statistics) {
        log_info("=== {} Compaction Statistics ===", db_name);
        log_info("No statistics available");
        log_info("==============================================");
        return;
    }

    uint64_t compact_read_bytes = statistics->getTickerCount(rocksdb::COMPACT_READ_BYTES);
    uint64_t compact_write_bytes = statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
    uint64_t compact_time_micros = statistics->getTickerCount(rocksdb::COMPACTION_TIME);

    log_info("=== {} Compaction Statistics ===", db_name);
    log_info("Compact Read Bytes: {} MB", compact_read_bytes / (1024 * 1024));
    log_info("Compact Write Bytes: {} MB", compact_write_bytes / (1024 * 1024));
    log_info("Compaction Time: {} ms", compact_time_micros / 1000);

    if (compact_read_bytes > 0) {
        // 改进的compaction count计算方法
        size_t compaction_count = 0;

        if (compact_time_micros > 0) {
            // 方法1：基于时间的估算（更准确）
            // 假设平均每次compaction需要100ms到1s，根据数据量动态调整
            uint64_t avg_compaction_time_micros = 500 * 1000; // 默认500ms

            // 根据compaction数据量调整平均时间估算
            if (compact_read_bytes > 1024 * 1024 * 1024) { // >1GB
                avg_compaction_time_micros = 1000 * 1000; // 1s for large compactions
            } else if (compact_read_bytes < 100 * 1024 * 1024) { // <100MB
                avg_compaction_time_micros = 200 * 1000; // 200ms for small compactions
            }

            compaction_count = compact_time_micros / avg_compaction_time_micros;
        } else {
            // 方法2：基于数据量的估算（fallback）
            // 动态调整平均compaction大小估算
            uint64_t avg_compaction_size = 64 * 1024 * 1024; // 默认64MB

            // 根据总数据量调整平均大小估算
            if (compact_read_bytes > 10ULL * 1024 * 1024 * 1024) { // >10GB
                avg_compaction_size = 128 * 1024 * 1024; // 128MB for large datasets
            } else if (compact_read_bytes < 500 * 1024 * 1024) { // <500MB
                avg_compaction_size = 32 * 1024 * 1024; // 32MB for small datasets
            }

            compaction_count = compact_read_bytes / avg_compaction_size;
        }

        // 确保至少有1次compaction
        if (compaction_count == 0) compaction_count = 1;

        log_info("Estimated Compaction Count: {}", compaction_count);
        log_info("Average Compaction Size: {} MB", compact_read_bytes / compaction_count / (1024 * 1024));
        log_info("Average Compaction Time: {} ms", compact_time_micros / compaction_count / 1000);
        log_info("Compaction Throughput: {} MB/s",
                compact_time_micros > 0 ?
                (compact_read_bytes / 1024.0 / 1024.0) / (compact_time_micros / 1000000.0) : 0.0);
    } else {
        log_info("No compaction activity recorded");
    }
    log_info("==============================================");
}

}