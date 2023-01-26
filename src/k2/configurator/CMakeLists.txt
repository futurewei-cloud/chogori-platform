file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(configurator OBJECT ${HEADERS} ${SOURCES})

target_link_libraries (configurator PRIVATE common config Seastar::seastar crc32c skvhttp::common skvhttp::mpack)

# export the library in the common k2Targets
install(TARGETS configurator EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/configurator)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS configurator NAMESPACE k2:: FILE configurator-config.cmake)
