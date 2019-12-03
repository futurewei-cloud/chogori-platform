#pragma once

#include "IndexerInterface.h"

#include <k2/common/Common.h>
#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>

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
    void insert(String key, String value, uint64_t version);

    VersionedTreeNode* find(const String& key, uint64_t version);

    void trim(const String& key, uint64_t version);
};

}
