file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(plog STATIC ${HEADERS} ${SOURCES})
target_link_libraries (plog PRIVATE transport common)

# export the library in the common k2Targets
install(TARGETS plog EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/plog)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS plog NAMESPACE k2:: FILE plog-config.cmake)
