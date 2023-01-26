add_executable (cpo_main cpo_main.cpp)

target_link_libraries (cpo_main PRIVATE appbase common configurator transport Seastar::seastar cpo_service infrastructure dto)

install (TARGETS cpo_main DESTINATION bin)
