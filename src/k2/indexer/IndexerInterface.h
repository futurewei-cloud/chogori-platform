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
#include <map>
#include <deque>
#include <utility>

#include <k2/common/Common.h>
#include <k2/dto/Collection.h>
#include <k2/dto/K23SI.h>
namespace k2
{
//
//  K2 internal MVCC representation
//

template<typename IndexerType, typename ValueType>
class Indexer
{
private:
  IndexerType idx;

public:
    typename IndexerType::iterator insert(dto::Key key);
    typename IndexerType::iterator find(dto::Key &key);
    typename IndexerType::iterator begin();
    typename IndexerType::iterator end();

    void erase(typename IndexerType::iterator it);
    size_t size();
};

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::insert(dto::Key key) {
    auto ret = idx.insert(std::pair<dto::Key, std::deque<ValueType>>(key, std::deque<ValueType>()));
    return ret.first;
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::find(dto::Key &key) {
    return idx.find(key);
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::begin() {
    return idx.begin();
}

template<typename IndexerType, typename ValueType>
inline typename IndexerType::iterator Indexer<IndexerType, ValueType>::end() {
    return idx.end();
}

template<typename IndexerType, typename ValueType>
inline void Indexer<IndexerType, ValueType>::erase(typename IndexerType::iterator it) {
    idx.erase(it);
}

template<typename IndexerType, typename ValueType>
inline size_t Indexer<IndexerType, ValueType>::size() {
    return idx.size();
}
}
