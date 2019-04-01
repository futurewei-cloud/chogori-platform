#pragma once

#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include "../common/Common.h"

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
    using KeyValuePairTrieType = hot::singlethreaded::HOTSingleThreaded<KeyValuePair*, KeyValuePairExtractor>;
    KeyValuePairTrieType m_keyValuePairTrie;
public:
    void insert(String key, String value, uint64_t version) {
        std::unique_ptr<Node> newNode(new Node);
        newNode->value = std::move(value);
        newNode->version = version;

        KeyValuePair* keyValuePair = new KeyValuePair();
        keyValuePair->key = std::move(key);
        keyValuePair->value = std::move(newNode);

        auto inserted = m_keyValuePairTrie.insert(keyValuePair);
        if(!inserted) //  Value already exists
        {
            auto exist = m_keyValuePairTrie.lookup(keyValuePair->key.c_str());
            newNode->next = std::move(exist.mValue->value);
            exist.mValue->value = std::move(newNode);
        }
    }

    Node* find(const String& key) {
        auto tuple = m_keyValuePairTrie.lookup(key.c_str());
        if (!tuple.mIsValid) {
            return nullptr;
        }
        return tuple.mValue->value.get();
    }

};

}