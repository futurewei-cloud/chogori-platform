#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->
if(DEFINED ENV{K2TX_DEBUG})
	set(K2TX_DEBUG $ENV{K2TX_DEBUG})
else()
    set(K2TX_DEBUG 0)
endif()

add_compile_definitions(K2TX_DEBUG=${K2TX_DEBUG})
add_executable (service service.cpp)

target_link_libraries (service PRIVATE transport Seastar::seastar )
