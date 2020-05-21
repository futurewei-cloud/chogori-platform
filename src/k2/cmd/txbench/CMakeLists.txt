add_executable (txbench_client txbench_client.cpp txbench_common.h)
add_executable (txbench_server txbench_server.cpp txbench_common.h)

add_executable (rpcbench_client rpcbench_client.cpp rpcbench_common.h)
add_executable (rpcbench_server rpcbench_server.cpp rpcbench_common.h)

add_executable (k23sibench_client k23sibench_client.cpp)

target_link_libraries (txbench_client PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (txbench_server PRIVATE k2appbase k2transport k2common Seastar::seastar)

target_link_libraries (rpcbench_client PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (rpcbench_server PRIVATE k2appbase k2transport k2common Seastar::seastar)

target_link_libraries (k23sibench_client PRIVATE k2appbase tso_clientlib k2cpo_client k23si_client)

install (TARGETS txbench_client txbench_server rpcbench_client rpcbench_server DESTINATION bin)
