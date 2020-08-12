file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (cpo_test ${HEADERS} ${SOURCES})

target_link_libraries (cpo_test PRIVATE k2appbase Seastar::seastar k2dto)
