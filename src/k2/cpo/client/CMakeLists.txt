file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2cpo_client STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2cpo_client PRIVATE k2common k2transport Seastar::seastar k2dto)
