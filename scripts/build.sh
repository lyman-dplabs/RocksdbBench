#!/bin/bash
set -e

# 检查参数
FORCE_REBUILD=false
if [ "$1" = "--force" ] || [ "$1" = "-f" ]; then
    FORCE_REBUILD=true
fi

echo "Building RocksDB benchmark..."

# 检查是否需要强制重新构建
if [ "$FORCE_REBUILD" = true ]; then
    echo "Force rebuild: removing build directory..."
    rm -rf build
fi

# 检查是否有新的CMakeLists.txt或源文件
if [ ! -d "build" ] || [ "CMakeLists.txt" -nt "build/CMakeCache.txt" ]; then
    echo "Configuring CMake..."
    cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=./vcpkg/scripts/buildsystems/vcpkg.cmake
    echo "CMake configured successfully!"
else
    echo "CMake cache is up to date"
fi

echo "Compiling..."
cmake --build build

echo "Build completed successfully!"
echo "Executable: ./build/src/rocksdb_bench_app"

# 显示使用提示
if [ "$FORCE_REBUILD" != true ]; then
    echo ""
    echo "提示: 使用 --force 或 -f 参数可以强制完全重新构建"
    echo "例如: ./scripts/build.sh --force"
fi