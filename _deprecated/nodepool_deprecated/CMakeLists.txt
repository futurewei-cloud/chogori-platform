#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (node_pool_deprecated ${HEADERS} ${SOURCES})
target_link_libraries (node_pool_deprecated PRIVATE k2transport k2node k2common k2indexer k2memkv_module_server Seastar::seastar)
install (TARGETS node_pool_deprecated DESTINATION bin)
