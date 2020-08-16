file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (payload_test ${HEADERS} ${SOURCES})

target_link_libraries (payload_test PRIVATE transport)
add_test(NAME transport COMMAND payload_test)
