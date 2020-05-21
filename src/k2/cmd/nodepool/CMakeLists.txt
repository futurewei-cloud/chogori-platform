add_executable (nodepool nodepool_main.cpp)

target_link_libraries (nodepool PRIVATE k2appbase k2common k2transport Seastar::seastar k2assignment_manager k2partition_manager k2node_pool_monitor k2collection_metadata_cache tso_clientlib)

install (TARGETS nodepool DESTINATION bin)
