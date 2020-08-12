file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k2cpo_service STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k2cpo_service PRIVATE k2common k2transport Seastar::seastar k2dto)

install(TARGETS k2cpo_service DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/cpo/service)
