add_executable (service service.cpp)
add_executable (rpc_service rpc_service.cpp)
add_executable (tso_sample_app tso_sample_app.cpp)
add_executable (txn_client_demo txn_client_demo.cpp)

target_link_libraries (service PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (rpc_service PRIVATE k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (tso_sample_app PRIVATE tso_service tso_clientlib k2appbase k2transport k2common Seastar::seastar)
target_link_libraries (txn_client_demo PRIVATE k2common Seastar::seastar)
