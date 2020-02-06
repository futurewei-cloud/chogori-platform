#pragma once
#include <k2/common/Common.h>

namespace k2
{
//
//  K2 internal MVCC representation
//
struct VersionedTreeNode {
    uint64_t version;
    String value;
    std::unique_ptr<VersionedTreeNode> next;
};

//
//  K2 internal indexing data structures interface
//
template <typename DerivedClass>
class IndexerInterface {
public:

    // add a new version of the key into the indexer
    void insert(String key, String value, uint64_t version) {
        static_cast<DerivedClass*>(this)->insert(std::move(key), std::move(value), version);
    }

    // find the specific version of the key from the indexer
    VersionedTreeNode* find(const String& key, uint64_t version) {
        return static_cast<DerivedClass*>(this)->find(key, version);
    }

    // trim the versions of the key older than the given parameter from the indexer
    void trim(const String& key, uint64_t version) {
        static_cast<DerivedClass*>(this)->trim(key, version);
    }
};
}
