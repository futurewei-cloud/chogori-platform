add_executable (plog_main plog_main.cpp)

target_link_libraries (plog_main PRIVATE appbase common configurator transport Seastar::seastar plog_service)

install (TARGETS plog_main DESTINATION bin)
