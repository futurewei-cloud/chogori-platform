add_executable (k2_demo_service service.cpp)
add_executable (k2_demo_rpc_echo rpc_echo.cpp)
add_executable (k2_demo_rpc_service rpc_service.cpp)
add_executable (k2_demo_tso tso_sample_app.cpp)
add_executable (k2_demo_txn_client txn_client_demo.cpp)

target_link_libraries (k2_demo_service PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (k2_demo_rpc_echo PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (k2_demo_rpc_service PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (k2_demo_tso PRIVATE k2tso_service k2tso_client k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (k2_demo_txn_client PRIVATE k2common Seastar::seastar)

install (TARGETS k2_demo_service k2_demo_rpc_echo k2_demo_rpc_service k2_demo_tso k2_demo_txn_client DESTINATION bin)
