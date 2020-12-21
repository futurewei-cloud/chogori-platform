add_executable (nodepool nodepool_main.cpp)

target_link_libraries (nodepool PRIVATE appbase common transport Seastar::seastar assignment_manager partition_manager node_pool_monitor collection_metadata_cache tso_client infrastructure)

install (TARGETS nodepool DESTINATION bin)
