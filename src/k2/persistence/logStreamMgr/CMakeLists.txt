file(GLOB HEADERS "LogStreamMgr.h")
file(GLOB SOURCES "LogStreamMgr.cpp")

add_library(log_stream_mgr STATIC ${HEADERS} ${SOURCES})
target_link_libraries (log_stream_mgr PRIVATE transport common cpo_client plog_client)

# export the library in the common k2Targets
install(TARGETS log_stream_mgr EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/logStream)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS log_stream_mgr NAMESPACE k2:: FILE log_stream_mgr-config.cmake)
