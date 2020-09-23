file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(config STATIC ${HEADERS} ${SOURCES})

set_target_properties(config PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (config PRIVATE common)

# export the library in the common k2Targets
install(TARGETS config EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/config)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS config NAMESPACE k2:: FILE config-config.cmake)
