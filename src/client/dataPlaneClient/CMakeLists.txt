cmake_minimum_required(VERSION 3.12)

add_executable (dataPlaneClient Main.cpp)
add_definitions(-DPARTITION_MANAGER_USE_OBS_INDEX)

target_link_libraries (dataPlaneClient PRIVATE k2common k2node boost_system boost_program_options boost_filesystem)
install (TARGETS dataPlaneClient DESTINATION bin)
