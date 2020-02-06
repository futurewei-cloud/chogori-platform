file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2indexer STATIC ${HEADERS} ${SOURCES})
