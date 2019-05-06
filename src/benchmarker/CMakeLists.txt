#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
project(k2benchmarker VERSION 1.0.0 DESCRIPTION "benchmarker library for k2 platform")

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2benchmarker STATIC ${HEADERS} ${SOURCES})

set_target_properties(
    k2benchmarker PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS k2benchmarker DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/benchmarker)
