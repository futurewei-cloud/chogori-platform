include_directories(include)
add_compile_options(-fno-var-tracking)

add_executable (mpack_test mpacktest.cpp)
add_executable (skv_record_test ${HEADERS} SKVRecordTest.cpp)
add_executable (expression_test ${HEADERS} ExpressionTest.cpp)
add_executable (client_integration_test ${HEADERS} ClientIntegrationTest.cpp)

target_link_libraries (mpack_test PRIVATE mpack)
target_link_libraries (skv_record_test PRIVATE dto mpack)
target_link_libraries (expression_test PRIVATE dto mpack)
target_link_libraries (client_integration_test PRIVATE client dto mpack)

add_test(NAME mpack_tst COMMAND mpack_test)
add_test(NAME skv_record COMMAND skv_record_test)
add_test(NAME expression COMMAND expression_test)
