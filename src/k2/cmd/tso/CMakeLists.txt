add_executable (tso tso_main.cpp)

target_link_libraries (tso PRIVATE k2tso_service k2appbase k2common k2transport Seastar::seastar)

install (TARGETS tso DESTINATION bin)
