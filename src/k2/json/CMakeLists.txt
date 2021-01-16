file(GLOB HEADERS "*.h")

add_library(json INTERFACE)

# export the library in the common k2Targets
install(TARGETS json EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/json)
# export the cmake json in the build tree for any users who want to use this project from source
export(TARGETS json NAMESPACE k2:: FILE json-config.cmake)
