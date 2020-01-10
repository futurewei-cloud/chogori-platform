#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (nodepool nodepool_main.cpp)

target_link_libraries (nodepool PRIVATE k2appbase k2common k2transport Seastar::seastar)

install (TARGETS nodepool DESTINATION bin)
