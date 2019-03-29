#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
project(transport VERSION 1.0.0 DESCRIPTION "transport library")

file(GLOB HEADERS "*.h")
file(GLOB IMPL "*.cpp")

add_library(transport STATIC ${HEADERS} ${IMPL})

if(DEFINED ENV{K2TX_DEBUG})
	set(K2TX_DEBUG $ENV{K2TX_DEBUG})
else()
    set(K2TX_DEBUG 0)
endif()

add_compile_definitions(K2TX_DEBUG=${K2TX_DEBUG})

set_target_properties(transport PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

install(TARGETS transport DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include)
