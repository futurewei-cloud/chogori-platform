file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2node STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2node PRIVATE k2common k2transport)

install(FILES ${HEADERS} DESTINATION include/k2/node)
