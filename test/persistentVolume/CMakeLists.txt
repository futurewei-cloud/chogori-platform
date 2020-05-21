add_executable (persistent_volume_test Main.cpp PersistentVolumeTest.cpp)

target_link_libraries (persistent_volume_test PRIVATE seastar_testing boost_unit_test_framework k2persistence k2plog k2common k2persistence stdc++fs Seastar::seastar)

add_test(NAME persistent_volume COMMAND persistent_volume_test -- --reactor-backend epoll)
