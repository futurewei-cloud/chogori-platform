add_executable (k23si_test K23SITest.cpp)
add_executable (read_cache_test ReadCacheTest.cpp)

target_link_libraries (k23si_test PRIVATE k2appbase Seastar::seastar k23si)
target_link_libraries (read_cache_test PRIVATE k23si)
add_test(NAME readcache COMMAND read_cache_test)
