#pragma once

#include "IndexerInterface.h"

namespace k2 
{
class MapIndexer : public IndexerInterface<MapIndexer> {
private:
    std::map<String, std::unique_ptr<Node>> m_map;
public:
    void insert(String key, String value, uint64_t version) {
        std::unique_ptr<Node> newNode(new Node);
        newNode->value = std::move(value);
        newNode->version = version;

        auto emplaceResult = m_map.try_emplace(std::move(key), std::move(newNode));
        //  Value already exists
        if (!emplaceResult.second) {
            newNode->next = std::move(emplaceResult.first->second);
            emplaceResult.first->second = std::move(newNode);
        }
    }

    Node* find(const String& key, uint64_t version) {
        auto it = m_map.find(key);
        if (it == m_map.end()) {
            return nullptr;
        }

        Node* node = it->second.get();
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

        Node* node = it->second.get();
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