#pragma once

#include "Module.h"

namespace k2 
{
//
//  K2 internal MVCC representation
//
struct Node {                
    uint64_t version;
    String value;
    std::unique_ptr<Node> next;
};

//
//  K2 internal indexing data structures interface
//
template <typename DerivedClass>
class IndexerInterface {
public:
    void insert(String key, String value, uint64_t version) {
        static_cast<DerivedClass*>(this)->insert(std::move(key), std::move(value), version);
    }

    Node* find(const String& key) {
        return static_cast<DerivedClass*>(this)->find(key);
    }

    // TODO: add the remove interface
};
}