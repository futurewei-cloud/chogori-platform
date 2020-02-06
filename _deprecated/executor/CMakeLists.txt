#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2executor STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2executor PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2executor PRIVATE k2common k2config k2transport boost_system boost_filesystem)

install(TARGETS k2executor DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/config)
