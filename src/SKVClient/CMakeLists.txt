cmake_minimum_required (VERSION 3.13)

project (skvlib VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)
add_compile_options(-O3 -std=c++17 -g -Werror -Wall -Wextra)

include_directories(src)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost COMPONENTS thread system REQUIRED) # header only libraries must not be added here
find_package(Threads REQUIRED)
find_package (fmt REQUIRED)

add_subdirectory (src)
add_subdirectory (test)

# install the cmake config for the entire project so that it can be used by upstream
# via find_package(skv REQUIRED)
# and pulling individual dependencies like so:
# DEPENDS ..... skv::dto skv::client
install(
    EXPORT skvTargets
    FILE skv-config.cmake
    DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/skv"
    NAMESPACE skv::
)
