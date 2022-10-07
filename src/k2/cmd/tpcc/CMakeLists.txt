file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tpcc_client ${HEADERS} ${SOURCES})

target_link_libraries (tpcc_client PRIVATE tso_client appbase transport common cpo_client k23si_client dto Seastar::seastar)

install (TARGETS tpcc_client DESTINATION bin)
