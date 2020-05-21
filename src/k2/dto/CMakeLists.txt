file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2dto STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2dto PRIVATE k2common)
