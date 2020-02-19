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

}   //  namespace k2
