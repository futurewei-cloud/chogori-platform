add_subdirectory (common)
add_subdirectory (cmd)

install (TARGETS master bench_client bench_server DESTINATION bin)
