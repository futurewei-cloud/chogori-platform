#pragma once

#include <functional>
#include <memory>
#include <chrono>
#include <iomanip>

#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/shared_ptr.hh>

#include <iostream>

#include <seastar/core/sstring.hh>

#include "Constants.h"
#include "Status.h"

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
//  Slice represents referenced (not owned) binary data
//
typedef seastar::temporary_buffer<char> Slice;

//
// The type for a function which can allocate Binary
//
typedef std::function<Binary()> BinaryAllocatorFunctor;

//
//  Endpoint identifies address of the Node or client. TODO: change to something more appropriate than 'String'.
//
typedef String Endpoint;

//
// duration used in a few places to specify timeouts and such
//
typedef std::chrono::steady_clock Clock;
typedef Clock::duration Duration;
typedef std::chrono::time_point<Clock> TimePoint;


//
//  Hold the reference to buffer containing class
//
template<typename T>
class Holder
{
protected:
    Binary data;
public:
    Holder(Binary&& binary):data(std::move(binary))
    {
        assert(data.size() >= sizeof(T));
    }

    Holder(Holder&& binary) = default;
    Holder& operator=(Holder&& other) = default;

    T& operator*() { return *(T*)data.get_write(); }
    T* operator->() { return (T*)data.get_write(); }
};

//  Binary which just reference some data. Owner of the data needs to make sure that when it delete the data
//  nobody has the reference to it
inline Binary binaryReference(void* data, size_t size)
{
    return Binary((char*)data, size, seastar::deleter());
}

template<typename CharT>
inline Binary binaryReference(seastar::temporary_buffer<CharT>& buffer, size_t offset, size_t size)
{
    assert(offset + size <= buffer.size());
    return binaryReference(buffer.get_write()+offset, size);
}

inline bool append(Binary& binary, size_t& writeOffset, const void* data, size_t size)
{
    if(binary.size() < writeOffset + size)
        return false;
    std::memcpy(binary.get_write() + writeOffset, data, size);
    writeOffset += size;
    return true;
}

template<typename T>
inline bool appendRaw(Binary& binary, size_t& writeOffset, const T& data)
{
    return append(binary, writeOffset, &data, sizeof(T));
}

}   //  namespace k2
