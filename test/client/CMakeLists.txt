add_executable (client_test PartitionRangeTest.cpp)
add_executable (executor1_test ExecutorThreadIntegTest.cpp)
add_executable (executor2_test ExecutorLoopIntegTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++14 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -faligned-new")

target_link_libraries (client_test PRIVATE k2client k2common)
target_link_libraries (executor1_test PRIVATE k2client k2common)
target_link_libraries (executor2_test PRIVATE k2client k2common)
