file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2persistence STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2persistence PRIVATE k2common)

install(TARGETS k2persistence DESTINATION lib)
