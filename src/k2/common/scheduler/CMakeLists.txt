project(k2scheduler VERSION 1.0.0 DESCRIPTION "scheduler library for k2 platform")

add_library(k2scheduler STATIC
    TcpService.cpp
)

set_target_properties(k2scheduler PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS k2scheduler DESTINATION lib/k2)
