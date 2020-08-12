file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2partition_manager STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2partition_manager PRIVATE k2common k2transport Seastar::seastar k2dto k23si)

install(TARGETS k2partition_manager DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/partitionManager)
