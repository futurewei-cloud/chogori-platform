file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(common STATIC ${HEADERS} ${SOURCES})

set_target_properties(common PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)
target_link_libraries (common PRIVATE Seastar::seastar fmt::fmt json)

# export the library in the common k2Targets
install(TARGETS common EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/common)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS common NAMESPACE k2:: FILE common-config.cmake)
