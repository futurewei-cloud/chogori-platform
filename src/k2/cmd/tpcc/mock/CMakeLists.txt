#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (mock_server mock_server.cpp)

target_link_libraries (mock_server PRIVATE k2appbase k2transport k2common Seastar::seastar)

install (TARGETS mock_server DESTINATION bin)
