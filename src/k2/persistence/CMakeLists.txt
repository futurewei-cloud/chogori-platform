add_subdirectory (plog)

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2persistence STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2persistence PRIVATE k2common k2plog)
