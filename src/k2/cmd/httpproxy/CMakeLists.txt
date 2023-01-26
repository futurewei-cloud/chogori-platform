add_executable (http_proxy http_proxy_main.cpp)

target_link_libraries (http_proxy PRIVATE appbase k23si_client cpo_client tso_client httpproxy infrastructure dto configurator transport Seastar::seastar)

install (TARGETS http_proxy DESTINATION bin)
