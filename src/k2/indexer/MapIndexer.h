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

#include "IndexerInterface.h"

namespace k2
{
template <typename ValueType>
class Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>
{
private:
    std::map<dto::Key, std::deque<ValueType>> idx;

public:
    typename std::map<dto::Key, std::deque<ValueType>>::iterator insert(dto::Key key);
    typename std::map<dto::Key, std::deque<ValueType>>::iterator find(dto::Key &key);
    typename std::map<dto::Key, std::deque<ValueType>>::iterator begin();
    typename std::map<dto::Key, std::deque<ValueType>>::iterator end();

    void erase(typename std::map<dto::Key, std::deque<ValueType>>::iterator it);
    size_t size();
};

template <typename ValueType>
inline typename std::map<dto::Key, std::deque<ValueType>>::iterator Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::insert(dto::Key key) {
    auto ret = idx.insert(std::pair<dto::Key, std::deque<ValueType>>(key, std::deque<ValueType>()));
    return ret.first;
}

template <typename ValueType>
inline typename std::map<dto::Key, std::deque<ValueType>>::iterator Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::find(dto::Key &key) {
    return idx.find(key);
}

template <typename ValueType>
inline void Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::erase(typename std::map<dto::Key, std::deque<ValueType>>::iterator it) {
    idx.erase(it);
}

template <typename ValueType>
inline typename std::map<dto::Key, std::deque<ValueType>>::iterator Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::begin() {
    return idx.begin();
}

template <typename ValueType>
inline typename std::map<dto::Key, std::deque<ValueType>>::iterator Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::end() {
    return idx.end();
}

template <typename ValueType>
inline size_t Indexer<std::map<dto::Key, std::deque<ValueType>>, ValueType>::size() {
    return idx.size();
}
}
