add_executable (client_test PartitionRangeTest.cpp)
add_executable (executor1_test ExecutorThreadIntegTest.cpp)
add_executable (executor2_test ExecutorLoopIntegTest.cpp)

target_link_libraries (client_test PRIVATE k2client k2common)
target_link_libraries (executor1_test PRIVATE k2client k2common)
target_link_libraries (executor2_test PRIVATE k2client k2common)
