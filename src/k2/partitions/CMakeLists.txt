#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2_partitions STATIC ${HEADERS} ${SOURCES})

set_target_properties(k2_partitions PROPERTIES
	VERSION ${PROJECT_VERSION}
	SOVERSION 1
)

target_link_libraries (k2_partitions PRIVATE k2common)

install(TARGETS k2_partitions DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/partitions)
