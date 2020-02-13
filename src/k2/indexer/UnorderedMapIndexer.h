#pragma once

#include "IndexerInterface.h"
#include <unordered_map>

namespace k2
{
template <typename ValueType>
class UnorderedMapIndexer {
   private:
    std::unordered_map<String, std::unique_ptr<VersionedTreeNode<ValueType>>> m_map;

   public:
    void insert(String key, ValueType value, uint64_t version);

    VersionedTreeNode<ValueType>* find(const String& key, uint64_t version);

    void trim(const String& key, uint64_t version);
};

template <typename ValueType>
inline void UnorderedMapIndexer<ValueType>::insert(String key, ValueType value, uint64_t version) {
    std::unique_ptr<VersionedTreeNode<ValueType>> newNode(new VersionedTreeNode < ValueType>());
    newNode->value = std::move(value);
    newNode->version = version;

    auto emplaceResult = m_map.try_emplace(std::move(key), std::move(newNode));
    //  Value already exists
    if (!emplaceResult.second) {
        newNode->next = std::move(emplaceResult.first->second);
        emplaceResult.first->second = std::move(newNode);
    }
}

template <typename ValueType>
inline VersionedTreeNode<ValueType>* UnorderedMapIndexer<ValueType>::find(const String& key, uint64_t version) {
    auto it = m_map.find(key);
    if (it == m_map.end()) {
        return nullptr;
    }

    VersionedTreeNode<ValueType>* node = it->second.get();
    while (node && node->version > version) {
        node = node->next.get();
    }
    return node;
}

template <typename ValueType>
inline void UnorderedMapIndexer<ValueType>::trim(const String& key, uint64_t version) {
    if (version == std::numeric_limits<uint64_t>::max()) {
        m_map.erase(key);
        return;
    }

    auto it = m_map.find(key);
    if (it == m_map.end()) {
        return;
    }

    VersionedTreeNode<ValueType>* node = it->second.get();
    if (node) {
        node = node->next.get();
    }
    while (node && node->version >= version) {
        node = node->next.get();
    }
    node->next = nullptr;
}
}
