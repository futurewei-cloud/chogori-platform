file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (mock_server ${HEADERS} ${SOURCES})

target_link_libraries (mock_server PRIVATE appbase transport common Seastar::seastar)

install (TARGETS mock_server DESTINATION bin)
