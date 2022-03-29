add_executable (http_proxy http_proxy_main.cpp)

target_link_libraries (http_proxy PRIVATE appbase tso_client httpproxy Seastar::seastar)

install (TARGETS http_proxy DESTINATION bin)
