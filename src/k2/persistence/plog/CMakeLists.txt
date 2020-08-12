file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2plog STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2plog PRIVATE k2transport k2common)

install(TARGETS k2plog DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/persistence/plog)
