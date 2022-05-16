file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(mpack STATIC ${HEADERS} ${SOURCES})

set_target_properties(mpack PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (mpack PRIVATE common)

# export the library in the common skvTargets
install(TARGETS mpack EXPORT skvTargets DESTINATION lib/skv)
install(FILES ${HEADERS} DESTINATION include/skv/mpack)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS mpack NAMESPACE skv:: FILE mpack-config.cmake)
