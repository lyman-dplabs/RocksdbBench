#!/bin/bash
set -e

DB_PATH=${1:-"./rocksdb_data"}

echo "Running RocksDB benchmark..."
echo "Database path: $DB_PATH"

if [ ! -f "./build/src/rocksdb_bench_app" ]; then
    echo "Error: Executable not found. Please run ./scripts/build.sh first."
    exit 1
fi

./build/src/rocksdb_bench_app "$DB_PATH"