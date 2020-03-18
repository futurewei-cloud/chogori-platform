file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k23si_client STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k23si_client PRIVATE k2common k2transport k2dto)
