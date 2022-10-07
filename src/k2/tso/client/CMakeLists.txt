file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(tso_client OBJECT ${HEADERS} ${SOURCES})

target_link_libraries (tso_client PRIVATE appbase dto config common transport Seastar::seastar)

# export the library in the common k2Targets
install(TARGETS tso_client EXPORT k2targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/tso/client)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS tso_client NAMESPACE k2:: FILE tso_client-config.cmake)
