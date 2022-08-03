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

#include <algorithm>
#include <decimal/decimal>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>

#include <skvhttp/common/Serialization.h>
#include <k2/logging/Chrono.h>

#define DISABLE_COPY(className)           \
    className(const className&) = delete; \
    className& operator=(const className&) = delete;

#define DISABLE_MOVE(className)      \
    className(className&&) = delete; \
    className& operator=(className&&) = delete;

#define DISABLE_COPY_MOVE(className) \
    DISABLE_COPY(className)          \
    DISABLE_MOVE(className)

#define DEFAULT_COPY(className)            \
    className(const className&) = default; \
    className& operator=(const className&) = default;

#define DEFAULT_MOVE(className)       \
    className(className&&) = default; \
    className& operator=(className&&) = default;

#define DEFAULT_COPY_MOVE(className) \
    DEFAULT_COPY(className)          \
    DEFAULT_MOVE(className)

#define DEFAULT_COPY_MOVE_INIT(className) \
    className() {}                        \
    DEFAULT_COPY(className)               \
    DEFAULT_MOVE(className)

namespace k2 {

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
typedef std::function<Binary(size_t)> BinaryAllocatorFunctor;

// define a binary allocator either by a default size or by a specific size
class BinaryAllocator {
public:
    // constructor without an allocation function
    BinaryAllocator() : _size(0), _func(nullptr) {
    }

    // pass in a default size and an allocation function
    BinaryAllocator(size_t size, BinaryAllocatorFunctor func) : _size(size), _func(func) {
    }

    // whether we can use this allocator
    bool canAllocate() const {
        return _func != nullptr;
    }

    // allocate by the default size
    Binary allocate() {
        return _func(_size);
    }

    // allocate by a specific size
    Binary allocate(size_t bsize) {
        return _func(bsize);
    }

private:
    // default allocation size
    size_t _size;
    // location function
    BinaryAllocatorFunctor _func;
};

class HexCodec {
private:
    inline static const int __k2__str_encode_bytesz = 4;
    inline static const char __k2__str_encode_char = '^';
    inline static const char* __k2_str_bytetohex[] = {
        "00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f",
        "10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f",
        "20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f",
        "30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f",
        "40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f",
        "50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f",
        "60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f",
        "70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f",
        "80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f",
        "90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f",
        "a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af",
        "b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf",
        "c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf",
        "d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df",
        "e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef",
        "f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"
    };

    inline static const int __k2_str__char2int[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 0-15
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 16-31
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 32-47
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0, // 48-63
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 64-79
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 80-95
        0, 10, 11, 12, 13, 14, 15, // 96-102
    };
public:
    inline static String encode(const char* data, size_t size) {
        static const int enc_byte_sz = 4;
        static const int bufsz = 3*enc_byte_sz;
        String result(String::initialized_later{}, size + bufsz + 1);
        size_t c = 0;
        for (size_t i = 0; i < size; ++i) {
            if (c > result.size() - enc_byte_sz) {
                // make sure we have enough space to write out a binary char+null with some factor to reduce
                // multiple allocations
                result.resize(c + 4*enc_byte_sz);
            }
            if (std::isprint(data[i])) {
                if (data[i] == __k2__str_encode_char) {
                    result[c++] = __k2__str_encode_char;
                }
                result[c++] = data[i];
            }
            else {
                result[c++] = __k2__str_encode_char;
                result[c++] = __k2_str_bytetohex[(uint8_t) data[i]][0];
                result[c++] = __k2_str_bytetohex[(uint8_t) data[i]][1];
            }
        }
        result[c] = '\0';
        result.resize(c);
        return result;
    }

    inline static String encode(const std::string& str) {
        return encode(str.data(), str.size());
    }

    inline static String encode(const String& str) {
        return encode(str.data(), str.size());
    }

    inline static String decode(const char* data, size_t size) {
        String result(String::initialized_later{}, size);
        size_t c = 0;
        for (size_t i = 0; i < size; ++i) {
            if (data[i] == __k2__str_encode_char) {
                ++i; // advance since we're skipping the encode char
                if (data[i] == __k2__str_encode_char) {
                    result[c++] = __k2__str_encode_char; // escaped encode char in original
                }
                else {
                    result[c++] = (__k2_str__char2int[(uint8_t)data[i]] << 4) + __k2_str__char2int[(uint8_t)data[i+1]];
                    ++i;
                }
            }
            else {
                result[c++] = data[i];
            }
        }
        result[c] = '\0';
        result.resize(c);
        return result;
    }

    inline static String decode(const std::string& str) {
        return decode(str.data(), str.size());
    }

    inline static String decode(const String& str) {
        return decode(str.data(), str.size());
    }
};

// this clock source should be used when you don't care just how precise you are with timing
// and you want to avoid a lot of calls to system's clock.
// It provides monotonically increasing, thread-local sequence of values and refreshes the
// system clock when asked.
struct CachedSteadyClock {
    typedef Duration duration;
    typedef Duration::rep rep;
    typedef Duration::period period;
    typedef TimePoint time_point;
    static const bool is_steady = true;

    static time_point now(bool refresh = true) noexcept;

   private:
    static inline thread_local TimePoint _now = Clock::now();
};

}  //  namespace k2

template <> // fmt support
struct fmt::formatter<k2::String> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(k2::String const& str, FormatContext& ctx) {
        k2::String encoded = k2::HexCodec::encode(str);
        return fmt::format_to(ctx.out(), "{}", encoded.data());
    }
};

template <> // fmt support
struct fmt::formatter<std::set<k2::String>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(std::set<k2::String> const& strset, FormatContext& ctx) {
        fmt::format_to(ctx.out(), "{{");
        size_t processed = 0;
        for(auto it = strset.cbegin(); it != strset.cend(); ++it) {
            if (processed == strset.size() - 1) {
                fmt::format_to(ctx.out(), "{}", *it);
            }
            else {
                fmt::format_to(ctx.out(), "{}, ", *it);
            }
        }
        return fmt::format_to(ctx.out(), "}}");
    }
};

template <> // fmt support
struct fmt::formatter<std::vector<k2::String>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(std::vector<k2::String> const& strvec, FormatContext& ctx) {
        fmt::format_to(ctx.out(), "{{");
        size_t processed = 0;
        for(auto it = strvec.cbegin(); it != strvec.cend(); ++it) {
            if (processed == strvec.size() - 1) {
                fmt::format_to(ctx.out(), "{}", *it);
            }
            else {
                fmt::format_to(ctx.out(), "{}, ", *it);
            }
        }
        return fmt::format_to(ctx.out(), "}}");
    }
};

template <> // fmt support for String
struct fmt::formatter<std::unordered_set<k2::String>> {
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(std::unordered_set<k2::String> const& strset, FormatContext& ctx) {
        fmt::format_to(ctx.out(), "{{");
        size_t processed = 0;
        for(auto it = strset.cbegin(); it != strset.cend(); ++it) {
            if (processed == strset.size() - 1) {
                fmt::format_to(ctx.out(), "{}", *it);
            }
            else {
                fmt::format_to(ctx.out(), "{}, ", *it);
            }
        }
        return fmt::format_to(ctx.out(), "}}");
    }
};

template <>  // serialization support for String
struct skv::http::Serializer<k2::String> {
    template <typename ReaderT>
    bool k2UnpackFrom(ReaderT& reader, k2::String& str) {
        typename ReaderT::Binary bin;
        if (!reader.read(bin)) {
            return false;
        }
        str = k2::String(bin.data(), bin.size());
        return true;
    }

    template <typename WriterT>
    void k2PackTo(WriterT& writer, const k2::String& str) const {
       typename WriterT::Binary bin(const_cast<char*>(str.data()), str.size(),[]{});
        writer.write(bin);
    }

    size_t k2GetNumberOfPackedFields() const {
        return 1;
    }
};
