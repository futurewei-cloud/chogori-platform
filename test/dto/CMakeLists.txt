file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (partition_test ${HEADERS} PartitionTest.cpp)

target_link_libraries (partition_test PRIVATE Seastar::seastar dto transport cpo_client tso_client k23si_client)