add_executable (cpo_main cpo_main.cpp)

target_link_libraries (cpo_main PRIVATE k2appbase k2common k2transport Seastar::seastar k2cpo_service)

install (TARGETS cpo_main DESTINATION bin)
