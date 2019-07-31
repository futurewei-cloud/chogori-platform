cmake_minimum_required(VERSION 3.12)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")
file(GLOB PERSISTANCE_HEADERS "persistence/*.h")
file(GLOB INDEXER_HEADERS "indexer/*.h")

add_library(k2node STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2node PRIVATE k2common)
add_subdirectory(main)
add_subdirectory(persistence)

install(FILES module/MemKVModule.h DESTINATION include/node/module)
install(FILES ${HEADERS} DESTINATION include/node)
install(FILES ${PERSISTANCE_HEADERS} DESTINATION include/node/persistence)
install(FILES ${INDEXER_HEADERS} DESTINATION include/node/indexer)
