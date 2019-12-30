#include "PartitionMeta.h"

namespace k2 {

const Partition* PartitionMap::getPartitionForKey(const String& key) const {
    auto iter = partitions.lower_bound(key);
    if (iter == partitions.end()) {
        return nullptr;
    }
    return iter->second;
}

} // namespace k2
