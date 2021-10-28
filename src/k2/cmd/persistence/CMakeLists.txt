add_executable (persistence persistence_main.cpp)

target_link_libraries (persistence PRIVATE cpo_client appbase common transport Seastar::seastar persistence_service)

install (TARGETS persistence DESTINATION bin)
