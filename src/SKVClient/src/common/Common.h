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

#include <boost/thread/future.hpp>

#include <decimal/decimal>
#include <iostream>
#include <string>

namespace std {
inline ostream& operator<<(ostream& os, const decimal::decimal64& d) {
    decimal::decimal64::__decfloat64 data = const_cast<decimal::decimal64&>(d).__getval();
    return os << (double)data;
}
inline ostream& operator<<(ostream& os, const decimal::decimal128& d) {
    decimal::decimal128::__decfloat128 data = const_cast<decimal::decimal128&>(d).__getval();
    return os << (double)data;
}
}

#define K2_DEF_ENUM(_K2_ENUM_TYPE_NAME,...) \
enum class _K2_ENUM_TYPE_NAME { \
    __VA_ARGS__ \
};

#define K2_SERIALIZABLE(...)                                                \
    template <typename Payload_T>                                           \
    void k2PackTo(Payload_T& ___k2_payload_local_macro_var___) const {      \
        ___k2_payload_local_macro_var___.writeMany(__VA_ARGS__);            \
    }                                                                       \
    template <typename Payload_T>                                           \
    bool k2UnpackFrom(Payload_T& ___k2_payload_local_macro_var___) {        \
        return ___k2_payload_local_macro_var___.readMany(__VA_ARGS__);      \
    }

#define FOR_EACH_1(what, x, ...) what(x)
#define FOR_EACH_2(what, x, ...) \
    what(x);                     \
    FOR_EACH_1(what, __VA_ARGS__);
#define FOR_EACH_3(what, x, ...) \
    what(x);                     \
    FOR_EACH_2(what, __VA_ARGS__);
#define FOR_EACH_4(what, x, ...) \
    what(x);                     \
    FOR_EACH_3(what, __VA_ARGS__);
#define FOR_EACH_5(what, x, ...) \
    what(x);                     \
    FOR_EACH_4(what, __VA_ARGS__);
#define FOR_EACH_6(what, x, ...) \
    what(x);                     \
    FOR_EACH_5(what, __VA_ARGS__);
#define FOR_EACH_7(what, x, ...) \
    what(x);                     \
    FOR_EACH_6(what, __VA_ARGS__);
#define FOR_EACH_8(what, x, ...) \
    what(x);                     \
    FOR_EACH_7(what, __VA_ARGS__);

#define FOR_EACH_NARG(...) FOR_EACH_NARG_(__VA_ARGS__, FOR_EACH_RSEQ_N())
#define FOR_EACH_NARG_(...) FOR_EACH_ARG_N(__VA_ARGS__)
#define FOR_EACH_ARG_N(_1, _2, _3, _4, _5, _6, _7, _8, N, ...) N
#define FOR_EACH_RSEQ_N() 8, 7, 6, 5, 4, 3, 2, 1, 0

#define FOR_EACH_(N, what, x, ...) CONCATENATE(FOR_EACH_, N)(what, x, __VA_ARGS__)
#define FOR_EACH(what, x, ...) FOR_EACH_(FOR_EACH_NARG(x, __VA_ARGS__), what, x, __VA_ARGS__)

#define K2_DEF_FMT(FormattableT, ...) \
    friend std::ostream& operator<<(std::ostream& os, const FormattableT& o) { \
        os << "{";
        FOR_EACH([&os](auto& x) { os << x << ", ";}, __VA_ARGS__); \
        return os << "}"; \
    }

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

namespace k2 {

using String = std::string;
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

template <typename ...T>
using Response = std::tuple<Status, T...>;

// Utility function to generate e response of a given type
template <typename ...T>
inline boost::future<Response<T...>> MakeResponse(Status s, T&&... r) {
    return make_ready_future(Response<T...>(std::move(s), std::forward<T>(r)...));
}

} // ns k2
