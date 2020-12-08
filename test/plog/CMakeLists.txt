file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (plog_test ${HEADERS} PlogClientTest.cpp)
add_executable (logstream_test ${HEADERS} LogStreamTest.cpp)

target_link_libraries (plog_test PRIVATE appbase transport common plog_client Seastar::seastar dto)
target_link_libraries (logstream_test PRIVATE appbase transport common log_stream_mgr Seastar::seastar dto)