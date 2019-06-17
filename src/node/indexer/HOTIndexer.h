#pragma once

#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#include <common/Common.h>

using namespace std;

namespace k2
{

struct KeyValuePair {
    String key;
    std::unique_ptr<VersionedTreeNode> value;
};

template<typename ValueType>
struct KeyValuePairKeyExtractor {
	inline size_t getKeyLength(ValueType const &value) const {
		return value->key.length();
	}
    inline const char* operator()(ValueType const &value) const {
        return value->key.c_str();
    }
};

class HOTIndexer : public IndexerInterface<HOTIndexer> {
    using KeyValuePairTrieType = hot::singlethreaded::HOTSingleThreaded<KeyValuePair*, KeyValuePairKeyExtractor>;
    KeyValuePairTrieType m_keyValuePairTrie;
public:
    void insert(String key, String value, uint64_t version) {
        std::unique_ptr<VersionedTreeNode> newNode(new VersionedTreeNode);
        newNode->value = std::move(value);
        newNode->version = version;

        KeyValuePair* keyValuePair = new KeyValuePair();
        keyValuePair->key = std::move(key);
        keyValuePair->value = std::move(newNode);

        auto inserted = m_keyValuePairTrie.insert(keyValuePair, keyValuePair->key.length());
        //  Value already exists
        if (!inserted) {
            auto exist = m_keyValuePairTrie.lookup(keyValuePair->key.c_str(), keyValuePair->key.length());
            newNode->next = std::move(exist.mValue->value);
            exist.mValue->value = std::move(newNode);
        }
    }

    VersionedTreeNode* find(const String& key, uint64_t version) {
        //
        // Bug: sometimes find() cannot find existing key
        //
        // auto it = m_keyValuePairTrie.find(key.c_str());
        // if (it == m_keyValuePairTrie.end()) {
        //     return nullptr;
        // }

        // VersionedTreeNode* node = (*it)->value.get();

        auto it = m_keyValuePairTrie.lookup(key.c_str(), key.length());
        if (!it.mIsValid) {
            return nullptr;
        }

        VersionedTreeNode* node = it.mValue->value.get();
        while (node && node->version > version) {
            node = node->next.get();
        }
        return node;
    }

    void trim(const String& key, uint64_t version) {
        if (version == std::numeric_limits<uint64_t>::max()) {
            auto it = m_keyValuePairTrie.lookup(key.c_str(), key.length());
            if (!it.mIsValid) {
                return;
            }
            m_keyValuePairTrie.remove(key.c_str(), key.length());
             return;
        }

        auto it = m_keyValuePairTrie.lookup(key.c_str(), key.length());
        if (!it.mIsValid) {
            return;
        }

        VersionedTreeNode* node = it.mValue->value.get();
        if (node) {
            node = node->next.get();
        }
        while (node && node->version >= version) {
            node = node->next.get();
        }
        node->next = nullptr;
    }

};

}