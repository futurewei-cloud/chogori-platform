cmake_minimum_required (VERSION 3.13)

project (k2_platform VERSION 0.0.1 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 17)
add_compile_options(-O3 -g -Werror -Wall -Wextra -march=skylake -mtune=skylake-avx512 -mavx -mavx2 -mxsave -mbmi -mbmi2 -mlzcnt -maes -mpopcnt)

# K2_VERBOSE_LOGGING enables some verbose logging for debug purposes. By default it is disabled and compiled out
if(DEFINED ENV{K2_VERBOSE_LOGGING})
    set(K2_VERBOSE_LOGGING $ENV{K2_VERBOSE_LOGGING})

else()
    set(K2_VERBOSE_LOGGING 0)
endif()

add_compile_definitions(K2_VERBOSE_LOGGING=${K2_VERBOSE_LOGGING})

# K2_MODULE_POOL_ALLOCATOR enables pool allocator for the module indexer. By default it is disabled
if(DEFINED ENV{K2_MODULE_POOL_ALLOCATOR})
    message("Found variable K2_MODULE_POOL_ALLOCATOR = '$ENV{K2_MODULE_POOL_ALLOCATOR}'")
    set(K2_MODULE_POOL_ALLOCATOR $ENV{K2_MODULE_POOL_ALLOCATOR})
else()
    set(K2_MODULE_POOL_ALLOCATOR 0)
endif()


add_compile_definitions(K2_MODULE_POOL_ALLOCATOR=${K2_MODULE_POOL_ALLOCATOR})

include_directories(src)

find_package(Seastar REQUIRED)
find_package(fmt REQUIRED)
find_package(skvhttp REQUIRED)
find_package(yule REQUIRED)

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
