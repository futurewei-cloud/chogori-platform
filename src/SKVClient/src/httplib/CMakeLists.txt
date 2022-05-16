file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(httplib STATIC ${HEADERS} ${SOURCES})

set_target_properties(httplib PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (httplib PRIVATE common)

# export the library in the common skvTargets
install(TARGETS httplib EXPORT skvTargets DESTINATION lib/skv)
install(FILES ${HEADERS} DESTINATION include/skv/httplib)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS httplib NAMESPACE skv:: FILE httplib-config.cmake)
