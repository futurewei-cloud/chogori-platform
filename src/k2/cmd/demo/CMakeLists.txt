add_executable (k2_demo_service service.cpp)
add_executable (k2_demo_rpc_echo rpc_echo.cpp)
add_executable (k2_demo_rpc_service rpc_service.cpp)
add_executable (k2_demo_tso tso_sample_app.cpp)
add_executable (configurator_client configurator_client.cpp)

target_link_libraries (configurator_client PRIVATE appbase configurator transport common Seastar::seastar)
target_link_libraries (k2_demo_service PRIVATE appbase configurator transport common Seastar::seastar)
target_link_libraries (k2_demo_rpc_echo PRIVATE appbase configurator transport common Seastar::seastar)
target_link_libraries (k2_demo_rpc_service PRIVATE appbase configurator transport common Seastar::seastar)
target_link_libraries (k2_demo_tso PRIVATE tso_service tso_client appbase configurator transport common Seastar::seastar)

#install (TARGETS k2_demo_service k2_demo_rpc_echo k2_demo_rpc_service k2_demo_tso DESTINATION bin)
