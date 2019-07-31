file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2plog STATIC ${HEADERS} ${SOURCES})

install(TARGETS k2plog DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include/common/plog)
