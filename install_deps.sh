#!/bin/bash
set -e

mkdir -p deps
cd deps
if [ ! -d "crc32c" ]; then git clone https://github.com/google/crc32c.git; fi
cd crc32c
git submodule update --init --recursive
mkdir -p build
cd build
cmake -DCRC32C_BUILD_TESTS=0 -DCRC32C_BUILD_BENCHMARKS=0 .. && make -j all install
cd ../../

if [ ! -d "chogori-intervaltree" ]; then git clone https://github.com/futurewei-cloud/chogori-intervaltree.git; fi
cd chogori-intervaltree
mkdir -p build
cd build
cmake .. && make -j install

