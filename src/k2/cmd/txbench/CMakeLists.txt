#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (txbench_client txbench_client.cpp txbench_common.h)
add_executable (txbench_server txbench_server.cpp txbench_common.h)


target_link_libraries (txbench_client PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (txbench_server PRIVATE k2appbase k2transport k2common Seastar::seastar)

install (TARGETS txbench_client txbench_server DESTINATION bin)
