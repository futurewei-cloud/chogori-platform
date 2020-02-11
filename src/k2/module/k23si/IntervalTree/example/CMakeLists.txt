cmake_minimum_required(VERSION 2.8.11)

project(Example CXX)

add_definitions(-std=c++11)

set(SOURCES main.cpp)

add_executable(${PROJECT_NAME} ${SOURCES})

target_link_libraries(${PROJECT_NAME} IntervalTree)
