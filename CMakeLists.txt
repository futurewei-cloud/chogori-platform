cmake_minimum_required (VERSION 3.13)

project (k2_platform VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -g -Werror -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)
#add_compile_options(-O0 -g -Werror -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

# K2_VERBOSE_LOGGING enables some verbose logging for debug purposes. By default it is disabled and compiled out
if(DEFINED ENV{K2_VERBOSE_LOGGING})
    set(K2_VERBOSE_LOGGING $ENV{K2_VERBOSE_LOGGING})
else()
    set(K2_VERBOSE_LOGGING 0)
endif()

add_compile_definitions(K2_VERBOSE_LOGGING=${K2_VERBOSE_LOGGING})

include_directories(src)

find_package (Seastar REQUIRED)
find_package (fmt REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package (yaml-cpp REQUIRED) # required for configuration file parsing

add_subdirectory (src)
add_subdirectory (test)

# install the cmake config for the entire project so that it can be used by upstream
# via find_package(k2 REQUIRED)
# and pulling individual dependencies like so:
# DEPENDS ..... k2::appbase k2::transport
install(
    EXPORT k2Targets
    FILE k2-config.cmake
    DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/k2"
    NAMESPACE k2::
)
