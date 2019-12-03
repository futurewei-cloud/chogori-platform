add_subdirectory(scheduler)
add_subdirectory(plog)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2common STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2common PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)
target_link_libraries (k2common PRIVATE Seastar::seastar )

install(TARGETS k2common DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/common)
