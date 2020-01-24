#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tso_main ${HEADERS} ${SOURCES})

target_link_libraries (tso_main PRIVATE k2appbase k2common k2transport Seastar::seastar)

install (TARGETS tso_main DESTINATION bin)

