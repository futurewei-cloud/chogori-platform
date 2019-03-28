#pragma once

#include "Module.h"

namespace k2 
{
struct Node {                
    uint64_t version;
    String value;
    std::unique_ptr<Node> next;
};

template <typename DerivedClass>
class MemtableInterface {
public:
    void insert(String key, String value, uint64_t version) {
        static_cast<DerivedClass*>(this)->insert(std::move(key), std::move(value), version);
    }

    Node* find(const String& key) {
        return static_cast<DerivedClass*>(this)->find(key);
    }
};
}