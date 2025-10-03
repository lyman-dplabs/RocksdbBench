#!/bin/bash

# 串行运行两个策略的基准测试脚本
# 使用nohup确保SSH断开后进程继续运行

set -e  # 遇到错误立即退出

# 提高文件描述符限制，避免"Too many open files"错误
ulimit -n 65536
echo "File descriptor limit set to: $(ulimit -n)"

# 创建日志目录
mkdir -p logs

# 获取时间戳
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "=========================================="
echo "Starting Sequential RocksDB Benchmark"
echo "Timestamp: $TIMESTAMP"
echo "=========================================="

# 策略1: Direct Version
echo ""
echo "=== Phase 1: Running Direct Version Strategy ==="
TIMESTAMP1=$(date +"%Y%m%d_%H%M%S")
echo "Start time: $(date)"
echo "Log file: logs/benchmark_direct_${TIMESTAMP1}.log"

# 运行direct版本（等待完成，不使用&）
nohup ./build/rocksdb_bench_app \
    --strategy direct_version \
    --batch-size-blocks 75000 \
    --max-batch-size-bytes 322122547200 \
    --duration 2 \
    --total-keys 1000 \
    --clean-data \
    > logs/benchmark_direct_${TIMESTAMP1}.log 2>&1

# 检查第一个策略是否成功完成
if [ $? -eq 0 ]; then
    echo "✓ Direct Version Strategy completed successfully!"
    echo "End time: $(date)"
else
    echo "✗ Direct Version Strategy failed!"
    echo "Check log: logs/benchmark_direct_${TIMESTAMP1}.log"
    exit 1
fi

# 等待一下，确保资源完全释放
sleep 5
rm -rf rocksdb_data

# 策略2: Dual RocksDB Adaptive
echo ""
echo "=== Phase 2: Running Dual RocksDB Adaptive Strategy ==="
TIMESTAMP2=$(date +"%Y%m%d_%H%M%S")
echo "Start time: $(date)"
echo "Log file: logs/benchmark_dual_${TIMESTAMP2}.log"

# 运行dual版本（等待完成，不使用&）
nohup ./build/rocksdb_bench_app \
    --strategy dual_rocksdb_adaptive \
    --batch-size-blocks 75000 \
    --max-batch-size-bytes 322122547200 \
    --enable-dynamic-cache-optimization \
    --clean-data \
    > logs/benchmark_dual_${TIMESTAMP2}.log 2>&1

# 检查第二个策略是否成功完成
if [ $? -eq 0 ]; then
    echo "✓ Dual RocksDB Adaptive Strategy completed successfully!"
    echo "End time: $(date)"
else
    echo "✗ Dual RocksDB Adaptive Strategy failed!"
    echo "Check log: logs/benchmark_dual_${TIMESTAMP2}.log"
    exit 1
fi

# 完成
echo ""
echo "=========================================="
echo "All benchmarks completed successfully!"
echo "Total duration: $SECONDS seconds"
echo "Direct log: logs/benchmark_direct_${TIMESTAMP1}.log"
echo "Dual log: logs/benchmark_dual_${TIMESTAMP2}.log"
echo "=========================================="

# 可选：发送完成通知（如果配置了邮件）
# echo "RocksDB benchmarks completed" | mail -s "Benchmark Complete" your-email@example.com