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
// field types (both primitive/simple as well as nested/complex)
// If your type is in fact trivial (POD), it will be (de)serialized as a direct copy
#define K2_SERIALIZABLE(Class_T, ...)                               \
    template <typename Writer_T>                                    \
    void k2PackTo(Writer_T& ___writer_local_macro_var___) const {   \
        ___writer_local_macro_var___.write(__VA_ARGS__);            \
    }                                                               \
    template <typename Reader_T>                                    \
    bool k2UnpackFrom(Reader_T& ___reader_local_macro_var___) {     \
        return ___reader_local_macro_var___.read(__VA_ARGS__);      \
    }                                                               \
    template <typename... T>                                        \
    size_t __k2GetNumberOfPackedFieldsHelper(T&...) const {         \
        return sizeof...(T);                                        \
    }                                                               \
    size_t k2GetNumberOfPackedFields() const {                      \
        return __k2GetNumberOfPackedFieldsHelper(__VA_ARGS__);      \
    }

// convenience macro - combines formattable and serializable traits
#define K2_SERIALIZABLE_FMT(Class_T, ...)  \
    K2_SERIALIZABLE(Class_T, __VA_ARGS__); \
    K2_DEF_FMT(Class_T, __VA_ARGS__);

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


// to tell if a type is read-serializable for a given reader type
template< class, class ReaderT, class = void >
struct isK2SerializableR : std::false_type {};

template<class T, class ReaderT>
struct isK2SerializableR<T, ReaderT,
 std::void_t<
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2UnpackFrom(std::declval<ReaderT&>())), bool>,void>,
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2GetNumberOfPackedFields()), std::size_t>,void>
  >> : std::true_type {};

// to tell if a type is read-serializable for a given reader type
template< class, class WriterT, class = void >
struct isK2SerializableW : std::false_type {};

template<class T, class WriterT>
struct isK2SerializableW<T, WriterT,
 std::void_t<
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2PackTo(std::declval<WriterT&>())), void>,void>,
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2GetNumberOfPackedFields()), std::size_t>,void>
  >> : std::true_type {};


template< class, class SerializerT, class = void >
struct isK2Serializable : std::false_type {};

template<class T, class SerializerT>
struct isK2Serializable<T, SerializerT,
 std::void_t<
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2UnpackFrom(std::declval<SerializerT&>())), bool>,void>,
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2PackTo(std::declval<SerializerT&>())), void>,void>,
    std::enable_if_t<std::is_same_v<
        decltype(std::declval<T>().k2GetNumberOfPackedFields()), std::size_t>,void>
  >> : std::true_type {};

template< class T >
struct isTrivialClass :
    std::integral_constant<
        bool,
        std::is_trivially_copyable<T>::value &&
        ! std::is_integral<T>::value &&
        ! std::is_enum<T>::value &&
        (sizeof(T) > 1)
    >
{};

// check if the given type has a complete implementation (e.g. particular specialization) of a template)
template <class T, std::size_t = sizeof(T)>
std::true_type isCompleteImpl(T*);
std::false_type isCompleteImpl(...);

template <class T>
using isCompleteType = decltype(isCompleteImpl(std::declval<T*>()));

template<typename T>
struct Serializer; // provide base, non-complete Serializer.
// External serialization can be provided by implementing, e.g.:
/*
template<>
struct k2::Serializer<MyType> {
    template <typename ReaderT>
    bool k2UnpackFrom(ReaderT& reader, MyType& o) {
        // read the data
        WriterT::Binary bin;
        if (!reader.read(bin)) {
            return false;
        }
        o = MyType(bin.data(), bin.size());
        return true;
    }

    template <typename WriterT>
    void k2PackTo(WriterT& writer, const MyType& o) const {
        // write the data
        WriterT::Binary bin(str.data(), str.size(),[]{});
        writer.write(bin);
    }

    size_t k2GetNumberOfPackedFields() const {
        // we serialize by reading/writing just one object (the Binary above)
        return 1;
    }
};
*/
} // ns skv::http
