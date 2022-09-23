file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(mpack STATIC ${HEADERS} ${SOURCES})

set_target_properties(mpack PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (mpack PRIVATE common Boost::serialization)

# export the library in the common skvhttpTargets
install(TARGETS mpack EXPORT skvhttpTargets DESTINATION lib/skvhttp)
install(FILES ${HEADERS} DESTINATION include/skvhttp/mpack)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS mpack NAMESPACE skvhttp:: FILE mpack-config.cmake)
