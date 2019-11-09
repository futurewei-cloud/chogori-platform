#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
project(k2client VERSION 1.0.0 DESCRIPTION "k2 client library")

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2client STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2client PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2client PRIVATE k2transport k2common k2config Seastar::seastar)

install(TARGETS k2client DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/client)
