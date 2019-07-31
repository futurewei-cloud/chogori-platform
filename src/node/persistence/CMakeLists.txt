file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2persistence STATIC ${HEADERS} ${SOURCES})

install(TARGETS k2persistence DESTINATION lib)
