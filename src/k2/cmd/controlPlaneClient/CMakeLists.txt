find_package (Boost REQUIRED)
add_executable (controlPlaneClient Main.cpp)

target_link_libraries (controlPlaneClient PRIVATE k2appbase k2common boost_system boost_program_options boost_filesystem Seastar::seastar)
install (TARGETS controlPlaneClient DESTINATION bin)
