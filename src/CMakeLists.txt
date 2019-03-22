add_subdirectory (common)
add_subdirectory (cmd)
add_subdirectory(transport)

install (TARGETS master bench_client bench_server demo_service DESTINATION bin)
