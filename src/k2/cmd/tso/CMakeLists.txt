add_executable (tso tso_main.cpp)

target_link_libraries (tso PRIVATE tso_service cpo_client appbase common configurator transport Seastar::seastar)

install (TARGETS tso DESTINATION bin)
