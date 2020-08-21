add_executable (tso tso_main.cpp)

target_link_libraries (tso PRIVATE tso_service appbase common transport Seastar::seastar)

install (TARGETS tso DESTINATION bin)
