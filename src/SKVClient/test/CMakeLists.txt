enable_testing()
include_directories(include)
add_compile_options(-fno-var-tracking)

add_executable (mpack_test mpacktest.cpp)
add_executable (skvrecord_test SKVRecordTest.cpp)

target_link_libraries (mpack_test PRIVATE mpack)
target_link_libraries (skvrecord_test PRIVATE dto mpack)

add_test(NAME mpack_tst COMMAND mpack_test)
add_test(NAME skvrecord_tst COMMAND skvrecord_test)
