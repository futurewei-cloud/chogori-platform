enable_testing()
include_directories(include)
add_compile_options(-fno-var-tracking)

add_executable (mpack_test mpacktest.cpp)
target_link_libraries (mpack_test PRIVATE mpack)
add_test(NAME mpack_tst COMMAND mpack_test)
