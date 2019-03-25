#pragma once

#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include "Common.h"
#include "Node.h"

using namespace std;

namespace k2 
{

struct KeyValuePair {
    String key;
    std::unique_ptr<Node> value;
};

template<typename ValueType>
struct KeyValuePairExtractor {
    typedef const char* KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key.c_str();
    }
};

class HOTMemtable : public MemtableInterface<HOTMemtable> {
    using KeyValuePairTrieType = hot::singlethreaded::HOTSingleThreaded<const KeyValuePair*, idx::contenthelpers::KeyValuePairExtractor>;
    KeyValuePairTrieType m_keyValuePairTrie;
public:
    std::unique_ptr<Node> insert(const String& key, const String& value, uint64_t version) {
        std::unique_ptr<Node> newNode(new Node);
        newNode->value = std::move(value);
        newNode->version = version;

        // BUGBUG
        KeyValuePair keyValuePair = { key, newNode };

        auto inserted = m_keyValuePairTrie.insert(&keyValuePair);
        if(!inserted.second)    //  Value already exists
        {
            newNode->next = std::move(inserted.first.value);
            inserted.first.value = std::move(newNode);
        }

        return newNode;
    }

    Node* find(const String& key) {
        auto t = m_keyValuePairTrie.lookup(key.c_str());
        if (!t.mIsValid) {
            return nullptr;
        }
        return t.mValue.value.get();
    }

}

}