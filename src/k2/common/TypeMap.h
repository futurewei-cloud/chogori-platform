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
