#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

file(GLOB HEADERS "*.h")
file(GLOB SOURCES "*.cpp")

add_executable (tso_service_main ${HEADERS} ${SOURCES})

target_link_libraries (tso_service_main PRIVATE tso_service k2appbase k2common k2transport Seastar::seastar)

install (TARGETS tso_service_main DESTINATION bin)
