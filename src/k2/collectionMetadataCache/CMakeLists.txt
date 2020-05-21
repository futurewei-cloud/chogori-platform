file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2collection_metadata_cache STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2collection_metadata_cache PRIVATE k2common k2transport Seastar::seastar k2dto)
