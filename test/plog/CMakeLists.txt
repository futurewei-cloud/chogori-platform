add_executable (plog_test Main.cpp PlogTest.cpp PlogTest.h)

target_link_libraries (plog_test PRIVATE k2appbase k2transport k2common k2plog Seastar::seastar k2dto)
