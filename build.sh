#!/bin/bash
set -e
cd "$(dirname "$0")"
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(sysctl -n hw.ncpu)
echo ""
echo "Built: build/can_reader, build/can_discover"
