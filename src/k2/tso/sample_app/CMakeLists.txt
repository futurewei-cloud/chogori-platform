#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tso_sampleapp ${HEADERS} ${SOURCES})

target_link_libraries (tso_sampleapp PRIVATE tso_service tso_clientlib k2appbase k2common k2transport Seastar::seastar)

install (TARGETS tso_sampleapp DESTINATION bin)

