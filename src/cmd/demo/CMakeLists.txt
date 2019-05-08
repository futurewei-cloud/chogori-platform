#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (service service.cpp)
add_executable (tcptest tcptest.cpp)

target_link_libraries (service PRIVATE k2transport k2common Seastar::seastar )
target_link_libraries (tcptest PRIVATE k2transport k2common Seastar::seastar )
