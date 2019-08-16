link_directories (${CMAKE_BINARY_DIR}/src/common/)

add_executable (config_test ConfigTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++14 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")

target_link_libraries (config_test PRIVATE k2config k2common Seastar::seastar)

install (TARGETS config_test DESTINATION bin)
