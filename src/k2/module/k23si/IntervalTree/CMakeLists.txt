#============================================================================
#
# Copyright (C) 2019 Ivan Pinezhaninov <ivan.pinezhaninov@gmail.com>
#
# This file is part of the IntervalTree - Red-Black balanced interval tree
# which can be found at https://github.com/IvanPinezhaninov/IntervalTree/.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
# THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
#============================================================================

cmake_minimum_required(VERSION 2.8.11)

project(IntervalTree CXX)

option(BUILD_EXAMPLE "Build example" OFF)
option(BUILD_TESTS "Build tests" OFF)

if(BUILD_EXAMPLE)
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/example)
endif(BUILD_EXAMPLE)

if(BUILD_TESTS)
  enable_testing()
  add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)
endif(BUILD_TESTS)

set(HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/intervaltree.hpp)

add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(${PROJECT_NAME} INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

install(FILES ${HEADERS} DESTINATION include)
