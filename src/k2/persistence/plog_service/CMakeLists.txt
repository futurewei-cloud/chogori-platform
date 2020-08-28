file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(plog_service STATIC ${HEADERS} ${SOURCES})
target_link_libraries (plog_service PRIVATE transport common cpo_client)

# export the library in the common k2Targets
install(TARGETS plog_service EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/plog_service)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS plog_service NAMESPACE k2:: FILE plog_service-config.cmake)
