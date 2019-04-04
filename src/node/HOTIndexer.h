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
struct KeyValuePairKeyExtractor {
    inline const char* operator()(ValueType const &value) const {
        return value->key.c_str();
    }
};

class HOTIndexer : public IndexerInterface<HOTIndexer> {
    using KeyValuePairTrieType = hot::singlethreaded::HOTSingleThreaded<KeyValuePair*, KeyValuePairKeyExtractor>;
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

    Node* find(const String& key, uint64_t version) {
        auto it = m_keyValuePairTrie.find(key.c_str());
        if(it == m_keyValuePairTrie.end()) {
            return nullptr;
        }

        Node* node = (*it)->value.get();
        while(node && node->version > version) {
            node = node->next.get();
        }
        return node;
    }

};

}