cmake_minimum_required (VERSION 3.5)

project (K2)

set (DPDK_SHLIBS "-lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_kvargs -lrte_eal -lrte_pci -lrte_bus_pci")
set (DPDK_LINK_FLAGS "-Wl,--no-as-needed ${DPDK_SHLIBS} -Wl,--as-needed")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${DPDK_LINK_FLAGS}")

include_directories(src)

find_package (Seastar REQUIRED)

add_subdirectory (src)
