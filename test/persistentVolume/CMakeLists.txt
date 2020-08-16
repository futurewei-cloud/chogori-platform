file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (persistent_volume_test ${HEADERS} ${SOURCES})

target_link_libraries (persistent_volume_test PRIVATE seastar_testing boost_unit_test_framework persistent_volume plog common stdc++fs Seastar::seastar)

add_test(NAME persistent_volume COMMAND persistent_volume_test -- --reactor-backend epoll)
