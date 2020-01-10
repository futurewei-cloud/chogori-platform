#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (cpo cpo_main.cpp)

target_link_libraries (cpo PRIVATE k2appbase k2common k2transport Seastar::seastar k2cpo_service)

install (TARGETS cpo DESTINATION bin)
