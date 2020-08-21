file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(partition_manager STATIC ${HEADERS} ${SOURCES})
target_link_libraries (partition_manager PRIVATE common transport Seastar::seastar dto k23si)

# export the library in the common k2Targets
install(TARGETS partition_manager EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/partitionManager)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS partition_manager NAMESPACE k2:: FILE partition_manager-config.cmake)
