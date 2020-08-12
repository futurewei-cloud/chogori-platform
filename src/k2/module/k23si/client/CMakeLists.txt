file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library(k23si_client STATIC ${HEADERS} ${SOURCES})
target_link_libraries (k23si_client PRIVATE k2common k2transport k2dto k2cpo_client)

install(TARGETS k23si_client DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/module/k23si/client)
