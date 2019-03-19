add_executable (bench_client bench_client.cpp)
add_executable (bench_server bench_server.cpp)

target_link_libraries (bench_client PRIVATE Seastar::seastar)
target_link_libraries (bench_server PRIVATE Seastar::seastar)
