<!--
    (C)opyright Futurewei Technologies Inc, 2019
-->

# chogori-platform
K2 Project is a platform for building low-latency (μs) in-memory distributed persistent OLTP databases.

This repository contains implementations for K2 core services and subsystems (transport, persistence, etc.).

## Build instructions

### Install instructions
 * Install chogori-seastar-rd (see instructions in the repo https://github.com/futurewei-cloud/chogori-seastar-rd)
 * run `./install_deps.sh` to install other dependency libraries
 * generate cmake and build `mkdir build && cd build && cmake .. && make -j`
 * run tests `cd build/test && ctest`
 * run integration tests `./test/integration/run.sh`
