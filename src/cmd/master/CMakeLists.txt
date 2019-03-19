#TODO: make common/scheduler a library

add_executable (master main.cpp ../../common/scheduler/RoundrobinScheduler.cpp ../../common/scheduler/SeastarScheduler.cpp)

target_link_libraries (master PRIVATE Seastar::seastar)
