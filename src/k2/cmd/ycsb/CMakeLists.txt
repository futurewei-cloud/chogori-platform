file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (ycsb_client ${HEADERS} ${SOURCES})

target_link_libraries (ycsb_client PRIVATE tso_client appbase transport common cpo_client k23si_client Seastar::seastar)

install (TARGETS ycsb_client DESTINATION bin)
