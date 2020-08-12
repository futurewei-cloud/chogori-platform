file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2transport STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2transport PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2transport PRIVATE k2common k2config Seastar::seastar  crc32c)

install(TARGETS k2transport DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/transport)
