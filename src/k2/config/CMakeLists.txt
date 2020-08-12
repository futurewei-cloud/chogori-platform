project(k2client VERSION 1.0.0 DESCRIPTION "k2 configuration")

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2config STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2config PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2config PRIVATE k2common)

install(TARGETS k2config DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/config)
