add_executable (dataPlaneClient Main.cpp)

target_link_libraries (dataPlaneClient PRIVATE k2common k2node boost_system boost_program_options boost_filesystem Seastar::seastar k2memkv_module_server)
install (TARGETS dataPlaneClient DESTINATION bin)
