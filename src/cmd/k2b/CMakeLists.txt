#<!--
#    (C)opyright Futurewei Technologies Inc, 2019
#-->

add_executable (k2b-service service/Service.cpp)

target_link_libraries (k2b-service PRIVATE k2benchmarker k2client boost_system boost_program_options boost_filesystem)
