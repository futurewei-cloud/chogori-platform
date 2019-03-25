#pragma once

#include "Module.h"

namespace k2 
{
struct Node {                
    uint64_t version;
    String value;
    std::unique_ptr<Node> next;
};

template <typename Derived>
class MemtableInterface {
public:
    std::unique_ptr<Node> insert(const String& key, const String& value, uint64_t version) {
        return static_cast<Derived*>(this)->insert(key, value, version);
    }

    Node* find(const String& key) {
        return static_cast<Derived*>(this)->find(key);
    }
};
}