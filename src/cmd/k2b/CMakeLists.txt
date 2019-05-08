#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (k2b-service Service.cpp)
add_executable (k2b-client Client.cpp)

target_link_libraries (k2b-service PRIVATE k2benchmarker k2transport Seastar::seastar)
target_link_libraries (k2b-client PRIVATE k2benchmarker k2transport Seastar::seastar)
