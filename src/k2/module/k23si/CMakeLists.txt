file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k23si STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k23si PRIVATE indexer common transport dto cpo_client log_stream)
add_subdirectory (client)

# export the library in the common k2Targets
install(TARGETS k23si EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/module/k23si)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS k23si NAMESPACE k2:: FILE k23si-config.cmake)
