file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(collection_metadata_cache STATIC ${HEADERS} ${SOURCES})
target_link_libraries (collection_metadata_cache PRIVATE common transport Seastar::seastar dto)

# export the library in the common k2Targets
install(TARGETS collection_metadata_cache EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/collectionMetadataCache)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS collection_metadata_cache NAMESPACE k2:: FILE collection_metadata_cache-config.cmake)
