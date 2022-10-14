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
#define BOOST_THREAD_PROVIDES_FUTURE
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION
#define BOOST_THREAD_PROVIDES_FUTURE_UNWRAP
#define BOOST_THREAD_PROVIDES_FUTURE_WHEN_ALL_WHEN_ANY

#include <k2/logging/Chrono.h>
#include <k2/logging/FormattingUtils.h>
#include <skvhttp/common/Serialization.h>

#include <boost/thread/future.hpp>


#ifdef _GLIBCXX_USE_DECIMAL_FLOAT
#include <boost/multiprecision/cpp_dec_float.hpp>
namespace skv::http {
using DecimalD50 = boost::multiprecision::cpp_dec_float_50;
using DecimalD100 = boost::multiprecision::cpp_dec_float_100;
// auto constexpr DecimalD50_to_float = std::decimal::decimal64_to_float; // NOT SURE
// auto constexpr DecimalD100_to_float = std::decimal::decimal128_to_float; // NOT SURE
}
#endif


namespace skv::http {
template <typename T>
auto make_ready_future(T&& obj) {
    boost::promise<T> prom;
    prom.set_value(std::forward<T>(obj));
    return prom.get_future();
}

template <typename T>
auto make_exc_future(std::exception_ptr p) {
    boost::promise<T> prom;
    prom.set_exception(std::move(p));
    return prom.get_future();
}

using String = std::string;
using Duration = k2::Duration;
using TimePoint = k2::TimePoint;
using Clock = k2::Clock;

class HexCodec {
   private:
    inline static const int __k2__str_encode_bytesz = 4;
    inline static const char __k2__str_encode_char = '^';
    inline static const char* __k2_str_bytetohex[] = {
        "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f",
        "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f",
        "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
        "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e", "3f",
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4a", "4b", "4c", "4d", "4e", "4f",
        "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
        "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f",
        "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a", "7b", "7c", "7d", "7e", "7f",
        "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
        "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b", "9c", "9d", "9e", "9f",
        "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af",
        "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
        "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb", "cc", "cd", "ce", "cf",
        "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de", "df",
        "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
        "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc", "fd", "fe", "ff"};

    inline static const int __k2_str__char2int[] = {
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 0-15
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 16-31
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 32-47
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 0, 0, 0, 0, 0,  // 48-63
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 64-79
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,  // 80-95
        0, 10, 11, 12, 13, 14, 15,                       // 96-102
    };

   public:
    inline static String encode(const char* data, size_t size) {
        static const int enc_byte_sz = 4;
        static const int bufsz = 3 * enc_byte_sz;
        String result(bufsz, 0);
        size_t c = 0;
        for (size_t i = 0; i < size; ++i) {
            if (c > result.size() - enc_byte_sz) {
                // make sure we have enough space to write out a binary char+null with some factor to reduce
                // multiple allocations
                result.resize(c + 4 * enc_byte_sz);
            }
            if (std::isprint(data[i])) {
                if (data[i] == __k2__str_encode_char) {
                    result[c++] = __k2__str_encode_char;
                }
                result[c++] = data[i];
            } else {
                result[c++] = __k2__str_encode_char;
                result[c++] = __k2_str_bytetohex[(uint8_t)data[i]][0];
                result[c++] = __k2_str_bytetohex[(uint8_t)data[i]][1];
            }
        }
        result[c] = '\0';
        result.resize(c);
        return result;
    }

    inline static String encode(const String& str) {
        return encode(str.data(), str.size());
    }

    inline static String decode(const char* data, size_t size) {
        String result;
        result.reserve(size);
        size_t c = 0;
        for (size_t i = 0; i < size; ++i) {
            if (data[i] == __k2__str_encode_char) {
                ++i;  // advance since we're skipping the encode char
                if (data[i] == __k2__str_encode_char) {
                    result[c++] = __k2__str_encode_char;  // escaped encode char in original
                } else {
                    result[c++] = (__k2_str__char2int[(uint8_t)data[i]] << 4) + __k2_str__char2int[(uint8_t)data[i + 1]];
                    ++i;
                }
            } else {
                result[c++] = data[i];
            }
        }
        result[c] = '\0';
        result.resize(c);
        return result;
    }

    inline static String decode(const String& str) {
        return decode(str.data(), str.size());
    }
};

template <typename Func>
class Defer {
public:
    Defer(Func&& func) : _func(std::forward<Func>(func)) {}
    ~Defer() {
        try {
            (void)_func();
        } catch (std::exception& exc) {
            std::cerr << "deferred func threw exception: {}" << exc.what() << std::endl;
        } catch (...) {
            std::cerr << "deferred func threw unknown exception" << std::endl;
        }
    }

   private:
    Func _func;
};
} // ns k2
