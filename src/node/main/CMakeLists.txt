#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (node_pool ${HEADERS} ${SOURCES})
target_link_libraries (node_pool PRIVATE k2transport k2node k2common Seastar::seastar)
install (TARGETS node_pool DESTINATION bin)
