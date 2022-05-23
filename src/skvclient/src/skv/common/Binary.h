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
#include <functional>
#include "Common.h"

namespace skv::http {
class Binary {
    typedef std::function<void()> Deleter;
public:
    Binary():_data(0),_size(0){};

    // Wrap a binary over the given size/data, which came from the given Binary source, thus sharing the buffer
    Binary(char* data, size_t size, Binary& source): Binary(data, size, source._state) {
    }

    // create a binary wrapping the given data and size. The deleter will be invoked when all shared instances are gone
    Binary(char* data, size_t size, Deleter d):
        Binary(data, size, std::make_shared<_SharedState>(std::move(d))) {
    }

    // wrap a binary around the given string.
    Binary(String&& str) {
        std::shared_ptr<String> ptr= std::make_shared<String>(std::move(str));
        _data = ptr->data();
        _size = ptr->size();
        this->_state = std::make_shared<_SharedState>([ptr=std::move(ptr)] {});
    }

    Binary(const Binary& o) = default;
    Binary(Binary&& o) = default;
    Binary& operator=(Binary&& o) = default;
    Binary& operator=(const Binary& o) = default;
    ~Binary() {}

    bool hasData() const {
        return _data != nullptr;
    }

    template <typename OStream_T>
    friend OStream_T& operator<<(OStream_T& os, const Binary& o) {
        (void)o;
        if constexpr (std::is_same<OStream_T, std::ostream>::value) {
            fmt::print(os,
                       FMT_STRING("{{binary({})}}"), o.size());
        } else {
            fmt::format_to(os.out(),
                           FMT_COMPILE("{{binary({})}}"), o.size());
        }
        return os;
    }

    size_t size() const {
        return _size;
    }

    char* data() {
        return _data;
    }

    const char* data() const {
        return _data;
    }

    Binary copy() const {
        char* newData = new char[_size];
        std::memcpy(newData, _data, _size);
        Binary b(newData, _size, [ptr=newData]() { delete ptr;});
        return b;
    }

private:
    struct _SharedState {
        template<typename Deleter>
        _SharedState(Deleter&& deleter): deleter(deleter){}
        Deleter deleter;

        ~_SharedState() {
            if (deleter) {
                deleter();
            }
        }
    };

    Binary(char* data, size_t size, const std::shared_ptr<_SharedState>& state): _data(data), _size(size), _state(state){}

    char* _data{nullptr};
    size_t _size{0};
    mutable std::shared_ptr<_SharedState> _state;
};

} // ns k2
