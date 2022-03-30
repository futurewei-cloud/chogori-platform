add_executable (http_client http_client.cpp)

target_link_libraries (http_client PRIVATE appbase tso_client cpo_client k23si_client dto infrastructure Seastar::seastar)

install (TARGETS http_client DESTINATION bin)

add_subdirectory(client)
