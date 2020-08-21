file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(persistence_service STATIC ${HEADERS} ${SOURCES})

set_target_properties(persistence_service PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (persistence_service PRIVATE common transport Seastar::seastar )

# export the library in the common k2Targets
install(TARGETS persistence_service EXPORT k2Targets DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/service)
# export the cmake config in the build tree for any users who want to use this project from source
export(TARGETS persistence_service NAMESPACE k2:: FILE persistence_service-config.cmake)
