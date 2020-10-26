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

namespace k2 {
// base recursion - no args to append
template <typename T>
void append_vec(std::vector<T>&) {}

// Append rvalues to a vector with variadic template expansion
template <typename T, typename... ObjT>
void append_vec(std::vector<T>& v, T&& arg, ObjT&&... args) {
    v.push_back(std::forward<T>(arg));
    append_vec(v, std::forward<ObjT>(args)...);
}

// create a vector using rvalues of type. This is needed to aid in creating rvalue vectors from
// non-copyable types since the standard lib initializer list doesn't support non-copyable types
template <typename T, typename... TPack>
std::vector<T> make_vec(TPack&&... objs) {
    std::vector<T> result;
    append_vec(result, std::forward<TPack>(objs)...);
    return result;
}
}
