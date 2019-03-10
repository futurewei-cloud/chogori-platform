cmake_minimum_required (VERSION 3.5)

project (K2)

set (DPDK_SHLIBS -lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_kvargs -lrte_eal -lrte_pci -lrte_bus_pci -lrte_pmd_mlx5)
add_definitions(-Wl,--no-as-needed ${DPDK_SHLIBS} -Wl,--as-needed)
include_directories(src)

find_package (Seastar REQUIRED)

add_executable (master src/cmd/master/main.cpp src/common/scheduler/roundrobin_scheduler.cc src/common/scheduler/seastar_scheduler.cc)
add_executable (benchmark src/cmd/benchmark/main.cpp)

target_link_libraries (master PRIVATE Seastar::seastar)
target_link_libraries (benchmark PRIVATE Seastar::seastar)

install (TARGETS master benchmark DESTINATION bin)
