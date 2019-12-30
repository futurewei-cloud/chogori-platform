#pragma once

#include "IndexerInterface.h"
#include <unordered_map>

namespace k2
{
class UnorderedMapIndexer : public IndexerInterface<UnorderedMapIndexer> {
private:
    std::unordered_map<String, std::unique_ptr<VersionedTreeNode>> m_map;
public:
    void insert(String key, String value, uint64_t version);

    VersionedTreeNode* find(const String& key, uint64_t version);

    void trim(const String& key, uint64_t version);
};
}
