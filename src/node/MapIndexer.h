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
        if(!emplaceResult.second)    //  Value already exists
        {
            newNode->next = std::move(emplaceResult.first->second);
            emplaceResult.first->second = std::move(newNode);
        }
    }

    Node* find(const String& key) {
        auto it = m_map.find(key);
        if(it == m_map.end()) {
            return nullptr;
        }
        return it->second.get();
    }
};
}