file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (cpo_test ${HEADERS} CPOTest.cpp Main.cpp)
add_executable(cpo_client_test CPOClientTest.cpp)

target_link_libraries (cpo_test PRIVATE appbase Seastar::seastar dto tso_client)
target_link_libraries (cpo_client_test PRIVATE appbase Seastar::seastar dto tso_client cpo_client transport common)