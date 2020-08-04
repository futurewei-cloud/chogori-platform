add_executable (k23si_test K23SITest.cpp)
add_executable (read_cache_test ReadCacheTest.cpp)
add_executable (serializable_document_test SerializableDocumentTest.cpp)

target_link_libraries (k23si_test PRIVATE k2appbase Seastar::seastar k23si)
target_link_libraries (read_cache_test PRIVATE k23si)
target_link_libraries (serializable_document_test PRIVATE k2appbase k2dto k2transport Seastar::seastar)
add_test(NAME readcache COMMAND read_cache_test)
add_test(NAME serializable_document COMMAND serializable_document_test)
