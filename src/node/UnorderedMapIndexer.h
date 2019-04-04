#pragma once

#include "IndexerInterface.h"

namespace k2 
{
class UnorderedMapIndexer : public IndexerInterface<UnorderedMapIndexer> {
private:
    std::unordered_map<String, std::unique_ptr<Node>> m_map;
public:
    void insert(String key, String value, uint64_t version) {
        std::unique_ptr<Node> newNode(new Node);
        newNode->value = std::move(value);
        newNode->version = version;

        auto emplaceResult = m_map.try_emplace(std::move(key), std::move(newNode));
        if(!emplaceResult.second)    //  Value already exists
        {
            newNode->next = std::move(emplaceResult.first->second);
            emplaceResult.first->second = std::move(newNode);
        }
    }

    Node* find(const String& key, uint64_t version) {
        auto it = m_map.find(key);
        if(it == m_map.end()) {
            return nullptr;
        }

        Node* node = it->second.get();
        while(node && node->version > version)
            node = node->next.get();
        return node;
    }
};
}