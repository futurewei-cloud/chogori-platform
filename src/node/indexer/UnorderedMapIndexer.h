#pragma once

#include "IndexerInterface.h"

namespace k2
{
class UnorderedMapIndexer : public IndexerInterface<UnorderedMapIndexer> {
private:
    std::unordered_map<String, std::unique_ptr<VersionedTreeNode>> m_map;
public:
    void insert(String key, String value, uint64_t version) {
        std::unique_ptr<VersionedTreeNode> newNode(new VersionedTreeNode);
        newNode->value = std::move(value);
        newNode->version = version;

        auto emplaceResult = m_map.try_emplace(std::move(key), std::move(newNode));
        //  Value already exists
        if (!emplaceResult.second) {
            newNode->next = std::move(emplaceResult.first->second);
            emplaceResult.first->second = std::move(newNode);
        }
    }

    VersionedTreeNode* find(const String& key, uint64_t version) {
        auto it = m_map.find(key);
        if (it == m_map.end()) {
            return nullptr;
        }

        VersionedTreeNode* node = it->second.get();
        while (node && node->version > version) {
            node = node->next.get();
        }
        return node;
    }

    void trim(const String& key, uint64_t version) {
        if (version == std::numeric_limits<uint64_t>::max()) {
            m_map.erase(key);
            return;
        }

        auto it = m_map.find(key);
        if (it == m_map.end()) {
            return;
        }

        VersionedTreeNode* node = it->second.get();
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