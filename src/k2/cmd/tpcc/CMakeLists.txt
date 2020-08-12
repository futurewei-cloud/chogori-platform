add_subdirectory (mock)
file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tpcc_client ${HEADERS} ${SOURCES})

target_link_libraries (tpcc_client PRIVATE k2tso_client k2appbase k2transport k2common k2cpo_client k23si_client Seastar::seastar)

install (TARGETS tpcc_client DESTINATION bin)
