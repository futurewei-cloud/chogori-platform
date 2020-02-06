#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2appbase STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2appbase PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2appbase PRIVATE k2config k2common k2transport Seastar::seastar )
