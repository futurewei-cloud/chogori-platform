add_executable (payload_test PayloadTest.cpp)

target_link_libraries (payload_test PRIVATE k2transport)
add_test(NAME transport COMMAND payload_test)
