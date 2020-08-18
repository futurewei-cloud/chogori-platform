file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2plog STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2plog PRIVATE k2transport k2common k2cpo_client k2dto)
