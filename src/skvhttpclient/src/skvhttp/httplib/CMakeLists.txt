file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(httplib STATIC ${HEADERS} ${SOURCES})

set_target_properties(httplib PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (httplib PRIVATE common)

# export the library in the common skvhttpTargets
install(TARGETS httplib EXPORT skvhttpTargets DESTINATION lib/skvhttp)
install(FILES ${HEADERS} DESTINATION include/skvhttp/httplib)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS httplib NAMESPACE skvhttp:: FILE httplib-config.cmake)
