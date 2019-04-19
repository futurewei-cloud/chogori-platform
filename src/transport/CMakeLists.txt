#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
project(k2transport VERSION 1.0.0 DESCRIPTION "transport library for k2 platform")

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2transport STATIC ${HEADERS} ${SOURCES})

if(DEFINED ENV{K2TX_DEBUG})
	set(K2TX_DEBUG $ENV{K2TX_DEBUG})
else()
    set(K2TX_DEBUG 0)
endif()

add_compile_definitions(K2TX_DEBUG=${K2TX_DEBUG})

set_target_properties(k2transport PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS k2transport DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/transport)
