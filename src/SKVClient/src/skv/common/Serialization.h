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

#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <deque>

// General purpose macro for creating serializable structures of any field types.
// You have to pass your fields here in order for them to be (de)serialized. This macro works for any
// field types (both primitive/simple as well as nested/complex) but it does the (de)serialization
// on a field-by-field basis so it may be less efficient than the one-shot macro below
#define K2_PAYLOAD_FIELDS(...)                                        \
    struct __K2PayloadSerializableTraitTag__ {};                      \
    template <typename Payload_T>                                     \
    void __k2PackTo(Payload_T& ___payload_local_macro_var___) const { \
        ___payload_local_macro_var___.write(__VA_ARGS__);             \
    }                                                                 \
    template <typename Payload_T>                                     \
    bool __k2UnpackFrom(Payload_T& ___payload_local_macro_var___) {   \
        return ___payload_local_macro_var___.read(__VA_ARGS__);       \
    }                                                                 \
    template <typename... T>                                          \
    size_t __k2GetNumberOfPackedFieldsHelper(T&...) const {                \
        return sizeof...(T);                                          \
    }                                                                 \
    size_t __k2GetNumberOfPackedFields() const {                      \
        return __k2GetNumberOfPackedFieldsHelper(__VA_ARGS__);        \
    }


namespace skv::http {
//
//  Serialization traits
//

template <typename T, typename Enabled=void>
struct isVectorLikeType : std::false_type {};

template <typename T>
struct isVectorLikeType<std::vector<T>> : std::true_type {};

template <typename T>
struct isVectorLikeType<std::list<T>> : std::true_type {};

template <typename T>
struct isVectorLikeType<std::deque<T>> : std::true_type {};

template <typename T>
struct isVectorLikeType<std::set<T>> : std::true_type {};

template <typename T>
struct isVectorLikeType<std::unordered_set<T>> : std::true_type {};

template <typename T, typename Enabled=void>
struct isMapLikeType : std::false_type {};

template <typename K, typename V>
struct isMapLikeType<std::map<K, V>> : std::true_type {};

template <typename K, typename V>
struct isMapLikeType<std::unordered_map<K, V>> : std::true_type {};

template <typename T, typename Enabled=void>
struct isPayloadSerializableType : std::false_type {};

template <typename T>
struct isPayloadSerializableType<T, std::void_t<typename T::__K2PayloadSerializableTraitTag__>> : std::true_type {};

} // ns k2
