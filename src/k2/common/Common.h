/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once

#include <functional>
#include <memory>
#include <iomanip>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/shared_ptr.hh>

#include <iostream>

#include <seastar/core/sstring.hh>

#include "Chrono.h"

#define DISABLE_COPY(className)                                 \
    className(const className&) = delete;                       \
    className& operator=(const className&) = delete;            \

#define DISABLE_MOVE(className)                                 \
    className(className&&) = delete;                            \
    className& operator=(className&&) = delete;                 \

#define DISABLE_COPY_MOVE(className)                            \
    DISABLE_COPY(className)                                     \
    DISABLE_MOVE(className)                                     \

#define DEFAULT_COPY(className)                                 \
    className(const className&) = default;                      \
    className& operator=(const className&) = default;           \

#define DEFAULT_MOVE(className)                                 \
    className(className&&) = default;                           \
    className& operator=(className&&) = default;                \

#define DEFAULT_COPY_MOVE(className)                            \
    DEFAULT_COPY(className)                                     \
    DEFAULT_MOVE(className)                                     \

#define DEFAULT_COPY_MOVE_INIT(className)                       \
    className() {}                                              \
    DEFAULT_COPY(className)                                     \
    DEFAULT_MOVE(className)                                     \

namespace k2
{

//
//  K2 general string type
//
typedef seastar::sstring String;

//
//  Binary represents owned (not referenced) binary data
//
typedef seastar::temporary_buffer<char> Binary;

//
// The type for a function which can allocate Binary
//
typedef std::function<Binary()> BinaryAllocatorFunctor;

// helper function for converting enum class into an integral type
// e.g. usage: auto integralColor = to_integral(MyEnum::Red);
// or  std::array<MyEnum, to_integral(MyEnum::Red)>;
template <typename T>
auto to_integral(T e) { return static_cast<std::underlying_type_t<T>>(e); }
}   //  namespace k2
