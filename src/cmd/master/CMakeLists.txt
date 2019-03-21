link_directories (${CMAKE_BINARY_DIR}/src/common/scheduler/)

add_executable (master main.cpp)

target_link_libraries (master PRIVATE scheduler Seastar::seastar)
