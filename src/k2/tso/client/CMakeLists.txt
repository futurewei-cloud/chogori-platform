file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2tso_client STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2tso_client PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2tso_client PRIVATE k2appbase k2dto k2config k2common k2transport Seastar::seastar)

install(TARGETS k2tso_client DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/tso/client)
