file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(httpproxy STATIC ${HEADERS} ${SOURCES})

target_link_libraries (httpproxy PRIVATE cpo_client k23si_client dto infrastructure common config Seastar::seastar skvhttp::dto)

# export the library in the common k2Targets
install(TARGETS httpproxy EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/httpproxy)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS httpproxy NAMESPACE k2:: FILE httpproxy-config.cmake)
