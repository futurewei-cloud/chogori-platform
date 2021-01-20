file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (common_test ${HEADERS} ${SOURCES})

target_link_libraries (common_test PRIVATE transport)
add_test(NAME transport COMMAND common_test)
