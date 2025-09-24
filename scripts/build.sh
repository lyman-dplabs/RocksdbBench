#!/bin/bash
set -e

echo "Building RocksDB benchmark..."

if [ ! -d "build" ]; then
    echo "Configuring CMake..."
    cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=./vcpkg/scripts/buildsystems/vcpkg.cmake
fi

echo "Compiling..."
cmake --build build

echo "Build completed successfully!"
echo "Executable: ./build/src/rocksdb_bench_app"