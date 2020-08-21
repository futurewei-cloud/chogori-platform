file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(node_pool_monitor STATIC ${HEADERS} ${SOURCES})
target_link_libraries (node_pool_monitor PRIVATE common transport Seastar::seastar dto)

# export the library in the common k2Targets
install(TARGETS node_pool_monitor EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/nodePoolMonitor)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS node_pool_monitor NAMESPACE k2:: FILE node_pool_monitor-config.cmake)
