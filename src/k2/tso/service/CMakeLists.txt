file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_library (tso_service STATIC ${HEADERS} ${SOURCES})

target_link_libraries (tso_service PRIVATE appbase common dto transport Seastar::seastar)

install(TARGETS tso_service DESTINATION lib/k2)
install(FILES ${HEADERS} DESTINATION include/k2/tso/service)
