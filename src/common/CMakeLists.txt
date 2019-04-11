add_subdirectory(scheduler)
project (common VERSION 1.0.0 DESCRIPTION "common library for k2 platform")

add_library (common STATIC
	Status.cpp
)

set_target_properties (common PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

file(GLOB HEADERS "*.h")

install (TARGETS common DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/common)
