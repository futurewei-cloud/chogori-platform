file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(dto STATIC ${HEADERS} ${SOURCES})

set_target_properties(dto PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (dto PRIVATE common)

# export the library in the common skvhttpTargets
install(TARGETS dto EXPORT skvhttpTargets DESTINATION lib/skvhttp)
install(FILES ${HEADERS} DESTINATION include/skvhttp/dto)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS dto NAMESPACE skvhttp:: FILE dto-config.cmake)
