#!/bin/bash
set -e

echo "Setting up vcpkg and installing dependencies..."

git submodule update --init --recursive

if [ ! -f "vcpkg/vcpkg" ]; then
    echo "Bootstrapping vcpkg..."
    cd vcpkg
    ./bootstrap-vcpkg.sh
    cd ..
fi

echo "Installing dependencies..."
./vcpkg/vcpkg install rocksdb fmt cli11 nlohmann-json

echo "Setup completed successfully!"