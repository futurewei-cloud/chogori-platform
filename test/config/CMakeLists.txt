add_executable (config_test ConfigTest.cpp)

target_link_libraries (config_test PRIVATE k2config k2common Seastar::seastar)
