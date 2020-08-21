file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (skv_record_test ${HEADERS} SKVRecordTest.cpp)
add_executable (key_encoding_test ${HEADERS} KeyEncodingTest.cpp)
add_executable (schema_creation_test ${HEADERS} SchemaCreationTest.cpp)
add_executable (skv_ser_test ${HEADERS} SKVSerTest.cpp)

target_link_libraries (skv_record_test PRIVATE dto transport)
target_link_libraries (key_encoding_test PRIVATE dto transport)
target_link_libraries (schema_creation_test PRIVATE appbase Seastar::seastar dto)
target_link_libraries (skv_ser_test PRIVATE dto transport)

add_test(NAME skv_record COMMAND skv_record_test)
add_test(NAME key_encoding COMMAND key_encoding_test)
add_test(NAME skv_ser COMMAND skv_ser_test)