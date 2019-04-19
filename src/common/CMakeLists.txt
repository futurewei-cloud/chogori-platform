add_subdirectory(scheduler)
project (k2common VERSION 1.0.0 DESCRIPTION "common library for k2 platform")

add_library (k2common STATIC
	Status.cpp
)

set_target_properties (k2common PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

file(GLOB HEADERS "*.h")

install (TARGETS k2common DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/common)
