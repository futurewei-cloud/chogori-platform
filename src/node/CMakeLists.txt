cmake_minimum_required(VERSION 3.12)

file(GLOB HEADERS "*.h")

find_package (Seastar REQUIRED)

add_executable (node_pool Main.cpp)

include_directories(..)

target_link_libraries (node_pool PRIVATE k2common Seastar::seastar)
install (TARGETS node_pool DESTINATION bin)
install(FILES module/MemKVModule.h DESTINATION include/node/module)
install(FILES seastar/SeastarPlatform.h DESTINATION include/node/seastar)
install(FILES ${HEADERS} DESTINATION include/node)