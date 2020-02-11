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

project(Test CXX)

add_definitions(-std=c++11)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/utils)

set(HEADERS
  utils/common.h
  utils/intervals.h
  utils/timer.h
  utils/random.h
)

set(SOURCES
  utils/intervals.cpp
  utils/timer.cpp
  utils/random.cpp
  benchmark.cpp
  constructors.cpp
  countinnerintervals.cpp
  countintervalscontainpoint.cpp
  countouterintervals.cpp
  countoverlappingintervals.cpp
  findinnerintervals.cpp
  findintervalscontainpoint.cpp
  findouterintervals.cpp
  findoverlappingintervals.cpp
  test.cpp
)

add_executable(${PROJECT_NAME} ${HEADERS} ${SOURCES})

target_link_libraries(${PROJECT_NAME} IntervalTree Catch)

add_test(NAME ${PROJECT_NAME} COMMAND ${PROJECT_NAME})
