#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library (tso_service STATIC ${HEADERS} ${SOURCES})

set_target_properties(tso_service PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1)

target_link_libraries (tso_service PRIVATE k2appbase k2common k2dto k2transport Seastar::seastar)

install(TARGETS tso_service DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/tso)


