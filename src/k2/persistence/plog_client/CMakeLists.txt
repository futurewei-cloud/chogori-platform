file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(plog_client STATIC ${HEADERS} ${SOURCES})
target_link_libraries (plog_client PRIVATE transport common cpo_client)

# export the library in the common k2Targets
install(TARGETS plog_client EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/plog_client)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS plog_client NAMESPACE k2:: FILE plog_client-config.cmake)
