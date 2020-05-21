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
#include <unordered_map>

namespace k2 {

// Hash-map for type->value mappings.
// Internally, we assign a unique id for each type and instantiate template methods as needed for the type
template <typename ValueT>
class TypeMap {
    typedef std::unordered_map<int, ValueT> __TypeMap;

   public:  // iterator stuff
    typedef typename __TypeMap::iterator iterator;
    typedef typename __TypeMap::const_iterator const_iterator;

    const_iterator begin() const { return _map.begin(); }
    const_iterator end() const { return _map.end(); }
    iterator begin() { return _map.begin(); }
    iterator end() { return _map.end(); }
    template <typename Key>
    iterator find() { return _map.find(getTypeId<Key>()); }
    template <typename Key>
    const_iterator find() const { return _map.find(getTypeId<Key>()); }
    // Associates a value with the type "Key"
    template <typename Key>
    void put(ValueT&& value) { _map.emplace(getTypeId<Key>(), std::forward<ValueT>(value)); }

   private:
    // This function creates the unique id for each type. It allows us to discover the id for any
    // type at runtime since the compiler generates a unique getTypeId for each type.
    template <typename Key>
    int getTypeId() {
        // the id for this type. Since this is static, it gets assigned exactly once, on first call with this type
        static const int id = _typeIdx++;
        return id;
    }
    int _typeIdx = 0;  // the latest id to use
    __TypeMap _map;
};

} // namespace k2
