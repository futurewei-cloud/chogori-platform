#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (service service.cpp)
add_executable (txbench_client txbench_client.cpp txbench_common.h)
add_executable (txbench_server txbench_server.cpp txbench_common.h)
add_executable (tcptest tcptest.cpp)
add_executable (txn_client_demo txn_client_demo.cpp)
add_executable (app app.cpp)


target_link_libraries (service PRIVATE k2transport k2common Seastar::seastar )
target_link_libraries (txbench_client PRIVATE k2transport k2common Seastar::seastar )
target_link_libraries (txbench_server PRIVATE k2transport k2common Seastar::seastar )
target_link_libraries (tcptest PRIVATE k2transport k2common Seastar::seastar )
target_link_libraries (txn_client_demo PRIVATE Seastar::seastar)
target_link_libraries (app PRIVATE k2transport k2common Seastar::seastar)

install (TARGETS txbench_client txbench_server DESTINATION bin)
