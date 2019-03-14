cmake_minimum_required (VERSION 3.5)

project (K2)

set (DPDK_SHLIBS -lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_kvargs -lrte_eal -lrte_pci -lrte_bus_pci)
set (DPDK_LINK_FLAGS -Wl,--no-as-needed ${DPDK_SHLIBS} -Wl,--as-needed)
set (CMAKE_LINKER_FLAGS_INIT ${DPDK_LINK_FLAGS})

include_directories(src)

find_package (Seastar REQUIRED)

add_executable (master src/cmd/master/main.cpp src/common/scheduler/roundrobin_scheduler.cc src/common/scheduler/seastar_scheduler.cc)
add_executable (bench_client src/cmd/benchmark/bench_client.cpp)
add_executable (bench_server src/cmd/benchmark/bench_server.cpp)

target_link_libraries (master PRIVATE Seastar::seastar)
target_link_libraries (bench_client PRIVATE Seastar::seastar)
target_link_libraries (bench_server PRIVATE Seastar::seastar)

install (TARGETS master bench_client bench_server DESTINATION bin)
