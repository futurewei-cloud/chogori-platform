

link_directories (${CMAKE_BINARY_DIR}/src/common/)

add_executable (plogmock_test Main.cpp PlogMockTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++17")

target_link_libraries (plogmock_test PRIVATE seastar_testing boost_unit_test_framework stdc++fs k2common Seastar::seastar plog)

install (TARGETS plogmock_test DESTINATION bin)
