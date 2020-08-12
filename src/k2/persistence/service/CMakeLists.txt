file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2persistence_service STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2persistence_service PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2persistence_service PRIVATE k2common k2transport Seastar::seastar )

install(TARGETS k2persistence_service DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/service)
