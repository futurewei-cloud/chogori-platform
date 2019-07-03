#pragma once

#include <string>
#include <functional>
#include <memory>
#include <chrono>
#include <cstring>
#include <iomanip>

#include <seastar/core/temporary_buffer.hh>

// We use OBS index layer as the partition management, and it is currently being built by gcc 5.3.0.
// gcc 5.3.0 can only support c++11/14. So we temporarily disable the c++17 features by using the below marco PARTITION_MANAGER_USE_OBS_INDEX
#ifndef PARTITION_MANAGER_USE_OBS_INDEX
#include <string_view>
#include <seastar/core/sstring.hh>
#endif

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
typedef std::string String;

//
//  Value of this type uniquely identifies K2 partition
//
typedef uint64_t PartitionId;

//
//  Binary represents owned (not referenced) binary data
//
typedef seastar::temporary_buffer<uint8_t> Binary;

//
//  Slice represents referenced (not owned) binary data
//
typedef seastar::temporary_buffer<uint8_t> Slice;

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
    Holder(Binary&& binary)
    {
        assert(data.size() >= sizeof(T));
        data = std::move(binary);
    }

    Holder(Holder&& binary) = default;
    Holder& operator=(Holder&& other) = default;

    T& operator*() { return *(T*)data.get_write(); }
    T* operator->() { return (T*)data.get_write(); }
};

//
//  Check whether some time interval is exceeded
//
class TimeTracker
{
    typedef std::chrono::steady_clock ClockT;
    typedef std::chrono::time_point<ClockT> TimePointT;

    TimePointT startTime;
    std::chrono::nanoseconds timeToTrack;

    static TimePointT now() { return ClockT::now(); }
public:
    TimeTracker() : startTime(std::chrono::nanoseconds::zero()), timeToTrack(std::chrono::nanoseconds::zero())  {}
    TimeTracker(std::chrono::nanoseconds timeToTrackNS) : startTime(now()), timeToTrack(timeToTrackNS)  {}
    TimeTracker(const TimeTracker& other) = default;

    bool exceeded() { return elapsed() > timeToTrack; };

    std::chrono::nanoseconds remaining()
    {
        TimePointT currentTime = now();
        auto elapsedTime = currentTime - startTime;

        if(elapsedTime > timeToTrack)
            return std::chrono::nanoseconds::zero();

        return timeToTrack - elapsedTime;
    }

    std::chrono::nanoseconds elapsed()
    {
        return now() - startTime;
    }
};

//  Seastar likes to use char for their buffers. Let make conversion easy.
template<typename T>
std::enable_if_t<std::is_arithmetic<T>::value && sizeof(T) == 1, Binary&> toBinary(seastar::temporary_buffer<T>& buffer)
{
    return *(Binary*)&buffer;
}

template<typename T>
Binary&& moveBinary(seastar::temporary_buffer<T>& buffer)
{
    return std::move(toBinary(buffer));
}

inline seastar::temporary_buffer<char>& toCharTempBuffer(Binary& buffer)
{
    return *(seastar::temporary_buffer<char>*)&buffer;
}

inline seastar::temporary_buffer<char>&& moveCharTempBuffer(Binary& buffer)
{
    return std::move(toCharTempBuffer(buffer));
}

//  Binary which just reference some data. Owner of the data needs to make sure that when it delete the data
//  nobody has the reference to it
inline Binary binaryReference(void* data, size_t size)
{
    return Binary((uint8_t*)data, size, seastar::deleter());
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

inline void print(std::ostream& stream, const void* buffer, size_t size)
{
    stream << '(' << size << ")[";

    bool printChars = false;
    for(size_t i = 0; i < size; i++)
    {
        char c = *((char*)buffer+i);
        if(isprint(c))
        {
            if(!printChars)
            {
                printChars = true;
                stream << '\'';
            }

            stream << c;
        }
        else
        {
            if(printChars)
            {
                stream << '\'';
                printChars = false;
            }

            stream << 'x' << std::setfill('0') << std::setw(2) << std::hex << std::uppercase << (uint32_t)(uint8_t)c;
        }
    }

    stream << ']';
}

inline std::ostream& operator<<(std::ostream& stream, const Binary& binary)
{
    print(stream, binary.get(), binary.size());
    return stream;
}

}   //  namespace k2
