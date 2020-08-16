file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (plogmock_test ${HEADERS} ${SOURCES})

target_link_libraries (plogmock_test PRIVATE seastar_testing boost_unit_test_framework plog transport stdc++fs common Seastar::seastar)

add_test(NAME plogmock COMMAND plogmock_test -- --reactor-backend epoll)
