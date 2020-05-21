add_executable (persistence persistence_main.cpp)

target_link_libraries (persistence PRIVATE k2appbase k2common k2transport Seastar::seastar k2persistence_service)

install (TARGETS persistence DESTINATION bin)
