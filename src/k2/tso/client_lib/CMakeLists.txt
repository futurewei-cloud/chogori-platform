#<!--
#    (C)opyright Futurewei Technologies Inc, 2020
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(tso_clientlib STATIC ${HEADERS} ${SOURCES})

set_target_properties(tso_clientlib PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (tso_clientlib PRIVATE k2appbase k2dto k2config k2common k2transport Seastar::seastar)

install(TARGETS tso_clientlib DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/tso)
