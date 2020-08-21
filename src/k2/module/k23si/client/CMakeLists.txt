file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k23si_client STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k23si_client PRIVATE common transport dto cpo_client)

# export the library in the common k2Targets
install(TARGETS k23si_client EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/module/k23si/client)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS k23si_client NAMESPACE k2:: FILE k23si_client-config.cmake)
