file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (configurator_test ${HEADERS} ConfiguratorTest.cpp)

target_link_libraries (configurator_test PRIVATE appbase dto configurator transport tso_client Seastar::seastar)
