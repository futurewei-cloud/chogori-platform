cmake_minimum_required (VERSION 3.12)

project (common VERSION 1.0.0 DESCRIPTION "common library for k2 platform")

add_library (common STATIC 
	Status.cpp
)

set_target_properties (common PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install (TARGETS common DESTINATION lib)
