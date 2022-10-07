file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(transport OBJECT ${HEADERS} ${SOURCES})

target_link_libraries (transport PRIVATE common config Seastar::seastar crc32c skvhttp::common skvhttp::mpack)

# export the library in the common k2Targets
install(TARGETS transport EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/transport)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS transport NAMESPACE k2:: FILE transport-config.cmake)
