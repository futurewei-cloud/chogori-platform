#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

cmake_minimum_required (VERSION 3.13)

project (k2_platform VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -Werror -Wall -Wextra)

set (DPDK_SHLIBS "-lrte_ethdev -lrte_net -lrte_mbuf -lrte_mempool -lrte_ring -lrte_mempool_ring -lrte_kvargs -lrte_eal -lrte_pci -lrte_bus_pci")
set (DPDK_LINK_FLAGS "-Wl,--no-as-needed ${DPDK_SHLIBS} -Wl,--as-needed")
set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${DPDK_LINK_FLAGS}")

include_directories(src)
include_directories(src/include)
include_directories(test/include)

find_package (Seastar REQUIRED)
find_package (yaml-cpp REQUIRED) # required for configuration file parsing

add_subdirectory (src)
add_subdirectory (test)
