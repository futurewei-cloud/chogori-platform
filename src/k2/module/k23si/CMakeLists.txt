file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_subdirectory (IntervalTree)
add_library(k23si STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k23si PRIVATE k2indexer k2common k2transport)
