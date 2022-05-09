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
#include <vector>

namespace k2::dto {

// base recursion - no args to append
template <typename T>
void _append_vec(std::vector<T>&) {}

// Append rvalues to a vector with variadic template expansion
template <typename T, typename... ObjT>
void _append_vec(std::vector<T>& v, T&& obj, ObjT&&... objs) {
    v.push_back(std::forward<T>(obj));
    _append_vec(v, std::forward<ObjT>(objs)...);
}

template <typename T, typename... ObjT>
void append_vec(std::vector<T>& v, ObjT&&... objs) {
    constexpr std::size_t sz = std::tuple_size<std::tuple<ObjT...>>::value;
    v.reserve(sz);
    _append_vec(v, std::forward<ObjT>(objs)...);
}

// create a vector using rvalues of type. This is needed to aid in creating rvalue vectors from
// non-copyable types since the standard lib initializer list doesn't support non-copyable types
template <typename T, typename... ObjT>
std::vector<T> make_vec(ObjT&&... objs) {
    std::vector<T> result;
    constexpr std::size_t sz = std::tuple_size<std::tuple<ObjT...>>::value;
    result.reserve(sz);
    append_vec(result, std::forward<ObjT>(objs)...);
    return result;
}
}
