cmake_minimum_required(VERSION 3.12)

find_package (Seastar REQUIRED)

add_executable (client Main.cpp)

include_directories(..)

target_link_libraries (client PRIVATE k2common k2client Seastar::seastar)
install (TARGETS client DESTINATION bin)
