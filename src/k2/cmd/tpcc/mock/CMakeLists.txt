file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (mock_server ${HEADERS} ${SOURCES})

target_link_libraries (mock_server PRIVATE k2appbase k2transport k2common Seastar::seastar)

install (TARGETS mock_server DESTINATION bin)
