cmake_minimum_required (VERSION 3.13)

project (yule VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_compile_options(-O3 -std=c++17 -g -Werror -Wall -Wextra)

# K2_VERBOSE_LOGGING enables some verbose logging for debug purposes. By default it is disabled and compiled out
if(DEFINED ENV{K2_VERBOSE_LOGGING})
    set(K2_VERBOSE_LOGGING $ENV{K2_VERBOSE_LOGGING})

else()
    set(K2_VERBOSE_LOGGING 0)
endif()

include_directories(src)

find_package (fmt REQUIRED)

add_subdirectory (src)

# install the cmake config for the entire project so that it can be used by upstream
# via find_package(yule REQUIRED)
# and pulling individual dependencies like so:
# DEPENDS ..... yule::logging
install(
    EXPORT yuleTargets
    FILE yule-config.cmake
    DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/yule"
    NAMESPACE yule::
)
