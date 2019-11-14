#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2transport STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2transport PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2transport PRIVATE k2common Seastar::seastar )

install(TARGETS k2transport DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/transport)
