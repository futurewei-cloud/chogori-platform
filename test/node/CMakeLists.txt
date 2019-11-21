add_executable (node_test Main.cpp AssignmentManagerTest.cpp)

target_link_libraries (node_test PRIVATE k2common k2node Seastar::seastar k2indexer k2memkv_module_server)
