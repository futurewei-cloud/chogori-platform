file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (partition_test ${HEADERS} PartitionTest.cpp)

target_link_libraries (partition_test PRIVATE appbase Seastar::seastar dto k23si_client tso_client)