cmake_minimum_required(VERSION 3.12)

find_package (Boost REQUIRED)
add_definitions(-DPARTITION_MANAGER_USE_OBS_INDEX)
add_executable (controlPlaneClient Main.cpp)

target_link_libraries (controlPlaneClient PRIVATE k2common k2boostclient boost_system boost_program_options boost_filesystem)
install (TARGETS controlPlaneClient DESTINATION bin)
