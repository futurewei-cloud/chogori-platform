cmake_minimum_required (VERSION 3.5)

project (K2)

find_package (Seastar REQUIRED)
add_executable (seastar_service src/seastar_service.cc src/common/scheduler/roundrobin_scheduler.cc src/common/scheduler/seastar_scheduler.cc) 
target_link_libraries (seastar_service PRIVATE Seastar::seastar)