file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(dto STATIC ${HEADERS} ${SOURCES})

set_target_properties(dto PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (dto PRIVATE common)

# export the library in the common skvTargets
install(TARGETS dto EXPORT skvTargets DESTINATION lib/skv)
install(FILES ${HEADERS} DESTINATION include/skv/dto)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS dto NAMESPACE skv:: FILE dto-config.cmake)
