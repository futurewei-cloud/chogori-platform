add_subdirectory(scheduler)
add_subdirectory(plog)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2common STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2common PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS k2common DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/common)
