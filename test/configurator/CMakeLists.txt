file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (configurator_test ${HEADERS} configurator_test.cpp)

target_link_libraries (configurator_test PRIVATE appbase dto configurator transport tso_client Seastar::seastar)

add_test(NAME configurator COMMAND configurator_test)
