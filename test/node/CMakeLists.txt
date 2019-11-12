add_executable (node_test Main.cpp AssignmentManagerTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++14 -march=broadwell -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")

target_link_libraries (node_test PRIVATE k2common k2node Seastar::seastar)
