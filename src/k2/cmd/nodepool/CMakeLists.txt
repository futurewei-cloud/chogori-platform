add_executable (nodepool nodepool_main.cpp)

target_link_libraries (nodepool PRIVATE appbase k23si cpo_client common configurator transport Seastar::seastar assignment_manager tso_client infrastructure dto)

install (TARGETS nodepool DESTINATION bin)
