/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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
#include <common/Binary.h>
#include <common/Common.h>
#include <common/Log.h>
#include <common/Serialization.h>

#include "mpack.h"

namespace k2 {
namespace log {
    inline thread_local k2::logging::Logger mpack("k2::mpack");
}

class MPackNodeReader {
public:
    MPackNodeReader(mpack_node_t node, Binary& source): _node(node), _source(source) {}

    // read something that isn't optional
    template <typename T>
    bool read(T& obj) {
        if (mpack_node_is_nil(_node)) {
            return false;
        }

        return _readFromNode(obj);
    }

    // read a value that can be optional (can be nil in the mpack stream)
    template <typename T>
    bool read(std::optional<T>& obj) {
        obj.reset();
        if (mpack_node_is_nil(_node)) {
            return true; // it's fine if the stored value is Nil - that'd be an empty optional
        }
        // value is not Nil so we'd better be able to read it as T
        T val;
        if (!_readFromNode(val)) {
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

public:
    class MPackStructReader {
    public:
        MPackStructReader(mpack_node_t& arrayNode, Binary& source) : _arrayNode(arrayNode), _idx(0), _source(source) {}

        template<typename T>
        bool read(T& obj) {
            mpack_node_t vnode = mpack_node_array_at(_arrayNode, _idx++);
            MPackNodeReader reader(vnode, _source);
            return reader.read(obj);
        }

        // convenience variadic read used to read more than one value at a time
        template <typename First_T, typename Second_T, typename... Rest_T>
        bool read(First_T& obj1, Second_T& obj2, Rest_T&... rest) {
            return read(obj1) && read(obj2) && read(rest...);
        }

        bool read() {return true;}

    private:
        mpack_node_t& _arrayNode;
        size_t _idx;
        Binary& _source;
    };

private:
    template <typename T>
    std::enable_if_t<isPayloadSerializableType<T>::value, bool>
    _readFromNode(T& val) {
        // PayloadSerializable types are packed as a list of values
        // get a reader for the node array and pass it to the struct itself for (recursive) unpacking
        MPackStructReader sreader(_node, _source);
        return val.__k2UnpackFrom(sreader);
    }

    bool _readFromNode(bool& val) {
        val = mpack_node_bool(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(uint8_t& val) {
        val = mpack_node_u8(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(int8_t& val) {
        val = mpack_node_i8(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(uint16_t& val) {
        val = mpack_node_u16(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(int16_t& val) {
        val = mpack_node_i16(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(uint32_t& val) {
        val = mpack_node_u32(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(int32_t& val) {
        val = mpack_node_i32(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(uint64_t& val) {
        val = mpack_node_u64(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(int64_t& val) {
        val = mpack_node_i64(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(float& val) {
        val = mpack_node_float_strict(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readFromNode(double& val) {
        val = mpack_node_double_strict(_node);
        return mpack_node_error(_node) == mpack_ok;
    }
    bool _readData(const char*& data, size_t& sz) {
        sz = mpack_node_bin_size(_node);
        if (mpack_node_error(_node) != mpack_ok) {
            return false;
        }
        data = mpack_node_bin_data(_node);
        if (mpack_node_error(_node) != mpack_ok) {
            return false;
        }

        return true;
    }
    bool _readFromNode(String& val) {
        // String is packed as a BINARY msgpack type
        size_t sz;
        const char* data;
        if (!_readData(data, sz)) {
            return false;
        }

        // TODO share ownership so we can avoid copy here
        val = String(data, sz);
        return true;
    }
    bool _readFromNode(Binary& val) {
        // binary is packed as a BINARY msgpack type
        size_t sz;
        const char* data;
        if (!_readData(data, sz)) {
            return false;
        }

        // TODO share ownership so we can avoid copy here
        val = Binary(const_cast<char*>(data), sz, _source);
        return true;
    }

    template <typename T>
    std::enable_if_t<isVectorLikeType<T>::value, bool>
    _readFromNode(T& val) {
        size_t sz = mpack_node_array_length(_node);
        for (size_t i = 0; i < sz; i++) {
            auto it = val.end();
            typename T::value_type v;
            MPackStructReader sreader(_node, _source);
            if (!sreader.read(v)) {
                val.clear();
                return false;
            }
            val.insert(it, std::move(v));
        }
        return true;
    }

    template <typename ...T>
    bool _readFromNode(std::tuple<T...>& val) {
        return std::apply([this](auto&&... args) mutable {
            return ((MPackStructReader(_node, _source).read(args)) && ...);
            },
            val);
    }

    template <typename T>
    std::enable_if_t<isMapLikeType<T>::value, bool>
    _readFromNode(T& val) {
        size_t sz = mpack_node_array_length(_node);
        for (size_t i = 0; i < sz; i++) {
            auto it = val.end();
            typename T::key_type k;
            typename T::mapped_type v;
            mpack_node_t element = mpack_node_array_at(_node, i);
            MPackStructReader sreader(element, _source);
            if (!sreader.read(k)) {
                val.clear();
                return false;
            }
            if (!sreader.read(v)) {
                val.clear();
                return false;
            }
            val.insert(it, {std::move(k), std::move(v)});
        }
        return true;
    }
    bool _readFromNode(Duration& dur) {
        if (typeid(std::remove_reference<decltype(dur)>::type::rep) != typeid(long int)) {
            return false;
        }
        long int ticks = 0;
        if (!read(ticks)) return false;
        dur = Duration(ticks);
        return true;
    }

    bool _readFromNode(std::decimal::decimal64& value) {
        // decimal is packed as a BINARY msgpack type
        size_t sz;
        const char* data;

        if (!_readData(data, sz)) {
            return false;
        }
        if (sizeof(std::decimal::decimal64::__decfloat64) != sz) {
            return false;
        }

        value.__setval(*((std::decimal::decimal64::__decfloat64*)data));
        return true;
    }

    bool _readFromNode(std::decimal::decimal128& value) {
        // decimal is packed as a BINARY msgpack type
        size_t sz;
        const char* data;

        if (!_readData(data, sz)) {
            return false;
        }
        if (sizeof(std::decimal::decimal128::__decfloat128) != sz) {
            return false;
        }

        value.__setval(*((std::decimal::decimal128::__decfloat128*)data));
        return true;
    }

    template <typename T>
    std::enable_if_t<std::is_enum<T>::value, bool>
    _readFromNode(T& value) {
        // enums are packed as a BINARY msgpack type
        size_t sz;
        const char* data;

        if (!_readData(data, sz)) {
            return false;
        }
        if (sizeof(T) != sz) {
            return false;
        }

        value = *(const T*)data;
        return true;
    }

private:
    mpack_node_t _node;
    Binary& _source;
};

class MPackReader {
public:
    MPackReader(){}
    MPackReader(const Binary& bin): _binary(bin){
        mpack_tree_init_data(&_tree, _binary.data(), _binary.size());  // initialize a parser + parse a tree
    }
    template<typename T>
    bool read(T& obj) {
        // read an entire node tree as a single object.
        mpack_tree_parse(&_tree);
        auto node = mpack_tree_root(&_tree);
        if (mpack_tree_error(&_tree) != mpack_ok) {
            return false;
        }
        MPackNodeReader reader(node, _binary);
        return reader.read(obj);
    }

private:
    Binary _binary;
    mpack_tree_t _tree;
};

class MPackNodeWriter {
public:
    MPackNodeWriter(mpack_writer_t& writer): _writer(writer){
    }

    void write(const Binary& val) {
        K2ASSERT(log::mpack, val.size() < std::numeric_limits<uint32_t>::max(), "cannot write binary of size {}", val.size());
        mpack_write_bin(&_writer, val.data(), (uint32_t)val.size());
    }

    template <typename T>
    void write(const std::optional<T>& obj) {
        if (!obj) {
            mpack_write_nil(&_writer);
        } else {
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
    std::enable_if_t<isPayloadSerializableType<T>::value, void>
    write(const T& val) {
        mpack_start_array(&_writer, val.__k2GetNumberOfPackedFields());
        val.__k2PackTo(*this);
        mpack_finish_array(&_writer);
    }

    template <typename T>
    std::enable_if_t<isVectorLikeType<T>::value, void>
    write(const T& val) {
        mpack_start_array(&_writer, val.size());
        for (const auto& el: val) {
            write(el);
        }
        mpack_finish_array(&_writer);
    }

    template <typename ...T>
    void write(const std::tuple<T...>& val) {
        mpack_start_array(&_writer, (uint32_t)std::tuple_size_v<std::tuple<T...>>);
        std::apply([this](auto&&... args) { write(args...); }, val);
        mpack_finish_array(&_writer);
    }

    template <typename T>
    std::enable_if_t<isMapLikeType<T>::value, void>
    write(const T& val) {
        mpack_start_array(&_writer, val.size());
        for (const auto& [k,v] : val) {
            mpack_start_array(&_writer, 2);
            write(k);
            write(v);
            mpack_finish_array(&_writer);
        }
        mpack_finish_array(&_writer);
    }

    void write(int8_t value) {
        mpack_write_i8(&_writer, value);
    }
    void write(uint8_t value) {
        mpack_write_u8(&_writer, value);
    }
    void write(int16_t value) {
        mpack_write_i16(&_writer, value);
    }
    void write(uint16_t value) {
        mpack_write_u16(&_writer, value);
    }
    void write(int32_t value) {
        mpack_write_i32(&_writer, value);
    }
    void write(uint32_t value) {
        mpack_write_u32(&_writer, value);
    }
    void write(int64_t value) {
        mpack_write_i64(&_writer, value);
    }
    void write(uint64_t value) {
        mpack_write_u64(&_writer, value);
    }
    void write(bool value) {
        mpack_write_bool(&_writer, value);
    }
    void write(float value) {
        mpack_write_float(&_writer, value);
    }
    void write(double value) {
        mpack_write_double(&_writer, value);
    }
    void write(const Duration& dur) {
        write(dur.count());  // write the tick count
    }
    void write(const std::decimal::decimal64& value) {
        std::decimal::decimal64::__decfloat64 data = const_cast<std::decimal::decimal64&>(value).__getval();
        mpack_write_bin(&_writer, (const char*)&data, sizeof(data));
    }
    void write(const std::decimal::decimal128& value) {
        std::decimal::decimal128::__decfloat128 data = const_cast<std::decimal::decimal128&>(value).__getval();
        mpack_write_bin(&_writer, (const char*)&data, sizeof(data));
    }
    void write(const String& val) {
        K2ASSERT(log::mpack, val.size() < std::numeric_limits<uint32_t>::max(), "cannot write binary of size {}", val.size());
        mpack_write_bin(&_writer, val.data(), (uint32_t)val.size());
    }

    template <typename T>
    std::enable_if_t<std::is_enum<T>::value, void>
    write(const T value) {
        mpack_write_bin(&_writer, (const char*)&value, sizeof(value));
    }

    void write(){
    }
private:
    mpack_writer_t &_writer;
};


class MPackWriter {
public:
    MPackWriter() {
        mpack_writer_init_growable(&_writer, &_data, &_size);
    }
    template <typename T>
    void write(T&& obj) {
        MPackNodeWriter writer(_writer);
        writer.write(std::forward<T>(obj));
    }

    void write(){
    }

    bool flush(Binary& binary) {
        if (mpack_writer_destroy(&_writer) != mpack_ok) {
            return false;
        }
        binary = Binary(_data, _size, [ptr=_data]() { MPACK_FREE(ptr);});
        _data = 0;
        _size = 0;
        return true;
    }
private:
    char* _data;
    size_t _size;
    mpack_writer_t _writer;
};
}
