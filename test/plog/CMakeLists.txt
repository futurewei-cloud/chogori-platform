add_executable (plog_test Main.cpp PlogTest.cpp PlogTest.h)

target_link_libraries (plog_test PRIVATE appbase transport common plog_client Seastar::seastar dto)
