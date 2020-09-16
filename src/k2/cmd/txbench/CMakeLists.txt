add_executable (txbench_client txbench_client.cpp txbench_common.h)
add_executable (txbench_server txbench_server.cpp txbench_common.h)
add_executable (txbench_combine txbench_combine.cpp txbench_common.h)

add_executable (rpcbench_client rpcbench_client.cpp rpcbench_common.h)
add_executable (rpcbench_server rpcbench_server.cpp rpcbench_common.h)

add_executable (k23sibench_client k23sibench_client.cpp)

target_link_libraries (txbench_client PRIVATE appbase transport common Seastar::seastar)
target_link_libraries (txbench_server PRIVATE appbase transport common Seastar::seastar)
target_link_libraries (txbench_combine PRIVATE appbase transport common Seastar::seastar)

target_link_libraries (rpcbench_client PRIVATE appbase transport common Seastar::seastar)
target_link_libraries (rpcbench_server PRIVATE appbase transport common Seastar::seastar)

target_link_libraries (k23sibench_client PRIVATE appbase tso_client cpo_client k23si_client dto)

install (TARGETS txbench_client txbench_server txbench_combine rpcbench_client rpcbench_server k23sibench_client DESTINATION bin)
