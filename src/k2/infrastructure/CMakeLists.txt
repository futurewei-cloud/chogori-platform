file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(infrastructure STATIC ${HEADERS} ${SOURCES})

target_link_libraries (infrastructure PRIVATE common config Seastar::seastar)

# export the library in the common k2Targets
install(TARGETS infrastructure EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/infrastructure)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS infrastructure NAMESPACE k2:: FILE infrastructure-config.cmake)
