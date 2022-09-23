cmake_minimum_required (VERSION 3.13)

project (skvhttpClient VERSION 0.0.1 LANGUAGES CXX)

##
## MAIN_PROJECT CHECK
## determine if we are built as a subproject (using add_subdirectory) or as the main project
##
set(MAIN_PROJECT OFF)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(MAIN_PROJECT ON)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_definitions(-DBOOST_ERROR_CODE_HEADER_ONLY)
add_compile_options(-O3 -g -Werror -Wall -Wextra)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(src)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost COMPONENTS thread system serialization REQUIRED) # header only libraries must not be added here
find_package(Threads REQUIRED)
find_package(fmt REQUIRED)
find_package(yule REQUIRED)

add_subdirectory (src)

enable_testing()
add_subdirectory (test)

# install the cmake config for the entire project so that it can be used by upstream
# via find_package(skv REQUIRED)
# and pulling individual dependencies like so:
# DEPENDS ..... skvhttp::dto skvhttp::client
install(
    EXPORT skvhttpTargets
    FILE skvhttp-config.cmake
    DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/skvhttp"
    NAMESPACE skvhttp::
)
