file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2indexer STATIC ${HEADERS} ${SOURCES})

install(TARGETS k2indexer DESTINATION lib/k2)
