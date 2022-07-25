file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (cpo_test ${HEADERS} CPOTest.cpp)
add_executable(cpo_client_test ${HEADERS} CPOClientTest.cpp)

target_link_libraries (cpo_test PRIVATE appbase Seastar::seastar dto tso_client)
target_link_libraries (cpo_test PRIVATE appbase Seastar::seastar dto tso_client cpo_client)
