file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (payload_test ${HEADERS} PayloadTest.cpp)
target_link_libraries (payload_test PRIVATE transport)
add_test(NAME transport_payload COMMAND payload_test)

add_executable (endpoint_test ${HEADERS} EndpointTest.cpp)
target_link_libraries (endpoint_test PRIVATE transport)
add_test(NAME transport_endpoint COMMAND endpoint_test)
