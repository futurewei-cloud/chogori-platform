file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(client STATIC ${HEADERS} ${SOURCES})

set_target_properties(client PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (client PRIVATE common dto mpack httplib)

# export the library in the common skvhttpTargets
install(TARGETS client EXPORT skvhttpTargets DESTINATION lib/skvhttp)
install(FILES ${HEADERS} DESTINATION include/skvhttp/client)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS client NAMESPACE skvhttp:: FILE client-config.cmake)
