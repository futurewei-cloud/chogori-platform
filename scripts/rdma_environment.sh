#!/bin/bash

# runs the docker container with appropriate flags to use RDMA
# the build folder will be mapped to /build
docker run --privileged --network=host -v "/dev/:/dev" -v "/sys/:/sys/" -v "${PWD}/../:/build" -w /usr/local/bin -it --rm -e LD_LIBRARY_PATH=/usr/local/lib -e RDMAV_HUGEPAGES_SAFE=1 -e MLX5_SINGLE_THREADED=1 k2-bvu-10001.huawei.com/seastar_sync_test:latest
