#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2partition_manager STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2partition_manager PRIVATE k2common k2transport Seastar::seastar k2dto)
