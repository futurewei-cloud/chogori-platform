file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(plog STATIC ${HEADERS} ${SOURCES})

install(FILES ${HEADERS} DESTINATION include/plog)
