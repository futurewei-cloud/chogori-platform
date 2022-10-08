file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(cpo_client OBJECT ${HEADERS} ${SOURCES})
target_link_libraries (cpo_client PRIVATE common Seastar::seastar dto transport)

# export the library in the common k2Targets
install(TARGETS cpo_client EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/cpo/client)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS cpo_client NAMESPACE k2:: FILE cpo_client-config.cmake)
