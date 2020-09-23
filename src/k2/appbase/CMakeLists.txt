file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(appbase STATIC ${HEADERS} ${SOURCES})

set_target_properties(appbase PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION 1
)

target_link_libraries (appbase PRIVATE config common transport Seastar::seastar )

# export the library in the common k2Targets
install(TARGETS appbase EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/appbase)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS appbase NAMESPACE k2:: FILE appbase-config.cmake)
