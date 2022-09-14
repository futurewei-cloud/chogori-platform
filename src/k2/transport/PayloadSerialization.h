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

#include <k2/logging/Log.h>
#include <skvhttp/common/Serialization.h>
#include "Payload.h"

// General purpose macro for creating serializable structures of any field types.
// You have to pass your fields here in order for them to be (de)serialized. This macro works for any
// field types (both primitive/simple as well as nested/complex) but it does the (de)serialization
// on a field-by-field basis so it may be less efficient than the one-shot macro below
#define K2_PAYLOAD_FIELDS(...)                                             \
    struct __K2PayloadSerializableTraitTag__ {};                           \
    void __writeFields(k2::Payload& ___payload_local_macro_var___) const { \
        ___payload_local_macro_var___.writeMany(__VA_ARGS__);              \
    }                                                                      \
    bool __readFields(k2::Payload& ___payload_local_macro_var___) {        \
        return ___payload_local_macro_var___.readMany(__VA_ARGS__);        \
    }                                                                      \
    size_t __getFieldsSize(k2::Payload& ___payload_local_macro_var___) {   \
        return ___payload_local_macro_var___.getFieldsSize(__VA_ARGS__);   \
    }

// This is a macro which can be put on structures which are directly copyable
// i.e. structures which can be copied by just casting the struct instance to a void* and
// copying some bytes:
// (void*)&structInstance, sizeof(structInstance)
// This is not going to work for structs which have complex nested fields (e.g. strings) as the content
// of the such nested field is in a different memory location. Use the general purpose macro above for
// these cases.
// In general, the simple type structs which can use K2_PAYLOAD_COPYABLE would be faster to serialize/deserialize
// as the entire struct is loaded/serialized in one single memory copy.
#define K2_PAYLOAD_COPYABLE struct __K2PayloadCopyableTraitTag__ {};

// macro useful for serializing empty classes/structs. C++ standard demands size to be at least 1, and will inject
// a dummy char if class/struct is empty anyway.
// we do the same here, but initialize this empty char so that we can validate message content on both sides
#define K2_PAYLOAD_EMPTY                  \
    char ___empty_payload_char___ = '\0'; \
    struct __K2PayloadCopyableTraitTag__ {};


namespace k2 {
// provide serializers for supporting K2_SERIALIZABLE types

class PayloadReader {
public:
    PayloadReader() {}
    PayloadReader(Payload& source) {
        _payload = source.shareAll();
        _payload.seek(source.getCurrentPosition());
    }

    // read a value that can be optional
    template <typename T>
    bool read(std::optional<T>& obj) {
        obj.reset();
        bool isPresent{false};
        if (!read(isPresent)) {
            return false;
        }
        if (!isPresent) {
            // we serialized an empty value. Return true and leave optional empty
            return true;
        }
        T val{};
        if (!read(val)) {
            return false;
        }
        obj = std::make_optional<T>(std::move(val));
        return true;
    }

    // convenience variadic read used to read more than one value at a time
    template <typename First_T, typename Second_T, typename... Rest_T>
    bool read(First_T& obj1, Second_T& obj2, Rest_T&... rest) {
        return read(obj1) && read(obj2) && read(rest...);
    }

    template <typename T>
    std::enable_if_t<skv::http::isK2SerializableR<T, PayloadReader>::value && !skv::http::isTrivialClass<T>::value, bool>
    read(T& val) {
        return val.k2UnpackFrom(*this);
    }

    template <typename T>
    std::enable_if_t<skv::http::isVectorLikeType<T>::value, bool>
    read(T& val) {
        size_t sz;
        if (!_payload.read(sz)) {
            return false;
        }

        for (size_t i = 0; i < sz; i++) {
            auto it = val.end();
            typename T::value_type v;
            if (!read(v)) {
                val.clear();
                return false;
            }
            val.insert(it, std::move(v));
        }
        return true;
    }

    template <typename... T>
    bool read(std::tuple<T...>& val) {
        return std::apply([this](auto&&... args) mutable {
            return ((read(args)) && ...);
        },
                          val);
    }

    template <typename T>
    std::enable_if_t<skv::http::isMapLikeType<T>::value, bool>
    read(T& val) {
        size_t sz;
        if (!_payload.read(sz)) {
            return false;
        }

        for (size_t i = 0; i < sz; i++) {
            auto it = val.end();
            typename T::key_type k;
            typename T::mapped_type v;
            if (!read(k)) {
                val.clear();
                return false;
            }
            if (!read(v)) {
                val.clear();
                return false;
            }
            val.insert(it, {std::move(k), std::move(v)});
        }
        return true;
    }


    bool read(bool& val) {
        return _payload.read(val);
    }
    bool read(uint8_t& val) {
        return _payload.read(val);
    }
    bool read(int8_t& val) {
        return _payload.read(val);
    }
    bool read(uint16_t& val) {
        return _payload.read(val);
    }
    bool read(int16_t& val) {
        return _payload.read(val);
    }
    bool read(uint32_t& val) {
        return _payload.read(val);
    }
    bool read(int32_t& val) {
        return _payload.read(val);
    }
    bool read(uint64_t& val) {
        return _payload.read(val);
    }
    bool read(int64_t& val) {
        return _payload.read(val);
    }
    bool read(float& val) {
        return _payload.read(val);
    }
    bool read(double& val) {
        return _payload.read(val);
    }
    bool read(String& val) {
        return _payload.read(val);
    }
    bool read(Binary& val) {
        return _payload.read(val);
    }
    bool read(Duration& dur) {
        return _payload.read(dur);
    }
    bool read(std::decimal::decimal64& value) {
        return _payload.read(value);
    }
    bool read(std::decimal::decimal128& value) {
        return _payload.read(value);
    }

    template <typename T>
    std::enable_if_t<std::is_enum<T>::value, bool>
    read(T& value) {
        return _payload.read((void*)&value, sizeof(T));
    }

    template <typename T>
    std::enable_if_t<skv::http::isTrivialClass<T>::value, bool>
    read(T& value) {
        return _payload.read((void*)&value, sizeof(T));
    }

private:
    Payload _payload;
};

class PayloadWriter {
public:
    PayloadWriter():_payload{Payload(Payload::DefaultAllocator())} {}
    PayloadWriter(Payload&& payload):_payload(std::move(payload)) {}
    Payload& getPayload() {
            return _payload;
    }

    template <typename T>
    void write(const std::optional<T>& obj) {
        K2LOG_V(log::tx, "writing nil optional of type {}", k2::type_name<T>());
        _payload.write(obj.has_value());

        if (obj) {
            write(*obj);
        }
    }

    // convenience variadic write used to write more than one value at a time
    template <typename First_T, typename Second_T, typename... Rest_T>
    void write(const First_T& obj1, const Second_T& obj2, const Rest_T&... rest) {
        write(std::forward<const First_T>(obj1));
        write(std::forward<const Second_T>(obj2));
        write(std::forward<const Rest_T>(rest)...);
    }

    template <typename T>
    std::enable_if_t<skv::http::isK2SerializableW<T, PayloadWriter>::value && !skv::http::isTrivialClass<T>::value, void>
    write(const T& val) {
        K2LOG_V(log::tx, "writing serializable type {}", val);
        val.k2PackTo(*this);
    }

    template <typename T>
    std::enable_if_t<skv::http::isVectorLikeType<T>::value, void>
    write(const T& val) {
        K2LOG_V(log::tx, "writing vector-like of type {} and size {}", k2::type_name<T>(), val.size());
        write(val.size());
        for (const auto& el : val) {
            write(el);
        }
    }

    template <typename T>
    std::enable_if_t<skv::http::isMapLikeType<T>::value, void>
    write(const T& val) {
        K2LOG_V(log::tx, "writing map-like of type {} and size {}", k2::type_name<T>(), val.size());
        write(val.size());
        for (const auto& [k, v] : val) {
            write(k);
            write(v);
        }
    }

    template <typename... T>
    void write(const std::tuple<T...>& val) {
        K2LOG_V(log::tx, "writing tuple of type {} and size {}", k2::type_name<std::tuple<T...>>(), std::tuple_size_v<std::tuple<T...>>);
        write(std::tuple_size_v<std::tuple<T...>>);
        std::apply([this](const auto&... args) { write(args...); }, val);
    }

    void write(int8_t value) {
        K2LOG_V(log::tx, "writing int8 type {}", value);
        _payload.write(value);
    }
    void write(uint8_t value) {
        K2LOG_V(log::tx, "writing uint8 type {}", value);
        _payload.write(value);
    }
    void write(int16_t value) {
        K2LOG_V(log::tx, "writing int16 type {}", value);
        _payload.write(value);
    }
    void write(uint16_t value) {
        K2LOG_V(log::tx, "writing uint16 type {}", value);
        _payload.write(value);
    }
    void write(int32_t value) {
        K2LOG_V(log::tx, "writing int32 type {}", value);
        _payload.write(value);
    }
    void write(uint32_t value) {
        K2LOG_V(log::tx, "writing uint32 type {}", value);
        _payload.write(value);
    }
    void write(int64_t value) {
        K2LOG_V(log::tx, "writing int64 type {}", value);
        _payload.write(value);
    }
    void write(uint64_t value) {
        K2LOG_V(log::tx, "writing uint64 type {}", value);
        _payload.write(value);
    }
    void write(bool value) {
        K2LOG_V(log::tx, "writing bool type {}", value);
        _payload.write(value);
    }
    void write(float value) {
        K2LOG_V(log::tx, "writing float type {}", value);
        _payload.write(value);
    }
    void write(double value) {
        K2LOG_V(log::tx, "writing double type {}", value);
        _payload.write(value);
    }
    void write(const Duration& value) {
        K2LOG_V(log::tx, "writing duration type {}", value);
        _payload.write(value);
    }
    void write(const std::decimal::decimal64& value) {
        K2LOG_V(log::tx, "writing decimald50 type {}", value);
        _payload.write(value);
    }
    void write(const std::decimal::decimal128& value) {
        K2LOG_V(log::tx, "writing decimald100 type {}", value);
        _payload.write(value);
    }
    void write(const String& value) {
        K2LOG_V(log::tx, "writing string type {}", value);
        _payload.write(value.data(), value.size());
    }
    void write(const Binary& val) {
        K2LOG_V(log::tx, "writing binary {}", val);
        _payload.write(val);
    }

    template <typename T>
    std::enable_if_t<std::is_enum<T>::value, void>
    write(const T value) {
        K2LOG_V(log::tx, "writing enum type {}", value);
        _payload.write((void*)&value, sizeof(T));
    }

    template <typename T>
    std::enable_if_t<skv::http::isTrivialClass<T>::value, void>
    write(const T& value) {
        K2LOG_V(log::tx, "writing enum type {}", value);
        _payload.write((void*)&value, sizeof(T));
    }

    void write() {
        K2LOG_V(log::tx, "writing base case");
    }

private:
    Payload _payload;
};
}
