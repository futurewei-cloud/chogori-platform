cmake_minimum_required(VERSION 3.12)

find_package (Seastar REQUIRED)

add_executable (node_pool Main.cpp)

include_directories(..)

target_link_libraries (node_pool PRIVATE common Seastar::seastar)
install (TARGETS node_pool DESTINATION bin)
install(FILES module/MemKVModule.h DESTINATION include/node/module)
