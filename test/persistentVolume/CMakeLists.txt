

link_directories (${CMAKE_BINARY_DIR}/src/common/)

add_executable (persistentvolume_test Main.cpp PersistentVolumeTest.cpp)
SET(CMAKE_CXX_FLAGS "-std=c++17")

target_link_libraries (persistentvolume_test PRIVATE seastar_testing boost_unit_test_framework k2persistence k2plog k2common stdc++fs Seastar::seastar)

install (TARGETS persistentvolume_test DESTINATION bin)
