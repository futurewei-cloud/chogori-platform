add_executable (plogmock_test Main.cpp PlogMockTest.cpp)

target_link_libraries (plogmock_test PRIVATE seastar_testing boost_unit_test_framework k2plog k2transport stdc++fs k2common Seastar::seastar)

add_test(NAME plogmock COMMAND plogmock_test -- --reactor-backend epoll)
