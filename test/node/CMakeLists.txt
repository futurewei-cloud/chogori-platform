

link_directories (${CMAKE_BINARY_DIR}/src/common/)

add_executable (node_test Main.cpp AssignmentManagerTest.cpp)

target_link_libraries (node_test PRIVATE common Seastar::seastar)

install (TARGETS node_test DESTINATION bin)
