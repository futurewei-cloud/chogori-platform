#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_subdirectory (service)

add_executable (tso_main tso_main.cpp)

target_link_libraries (tso_main PRIVATE k2transport k2common k2transport Seastar::seastar k2tso_service)

install (TARGETS tso_main DESTINATION bin)
