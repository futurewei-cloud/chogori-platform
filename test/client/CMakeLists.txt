link_directories (${CMAKE_BINARY_DIR}/src/common/)

add_executable (client_test PartitionRangeTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++14 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")

target_link_libraries (client_test PRIVATE k2client Seastar::seastar)

install (TARGETS client_test DESTINATION bin)
