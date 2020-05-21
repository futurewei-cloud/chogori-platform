add_subdirectory (mock)

add_executable (tpcc_client tpcc_client.cpp)

target_link_libraries (tpcc_client PRIVATE tso_clientlib k2appbase k2transport k2common k2cpo_client k23si_client Seastar::seastar)

install (TARGETS tpcc_client DESTINATION bin)
