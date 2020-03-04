#include "Collection.h"
#include <crc32c/crc32c.h>
#include <string>
#include <algorithm>

namespace k2 {
namespace dto {

int Key::compare(const Key& o) const noexcept {
    auto pkcomp = partitionKey.compare(o.partitionKey);
    if (pkcomp == 0) {
        // if the partition keys are equal, return the comparison of the range keys
        return rangeKey.compare(o.rangeKey);
    }
    return pkcomp;
}

bool Key::operator<(const Key& o) const noexcept {
    return compare(o) < 0;
}
bool Key::operator<=(const Key& o) const noexcept {
    return compare(o) <= 0;
}
bool Key::operator>(const Key& o) const noexcept {
    return compare(o) > 0;
}
bool Key::operator>=(const Key& o) const noexcept {
    return compare(o) >= 0;
}
bool Key::operator==(const Key& o) const noexcept {
    return compare(o) == 0;
}
size_t Key::hash() const noexcept {
    return std::hash<k2::String>()(partitionKey) + std::hash<k2::String>()(rangeKey);
}

bool Partition::PVID::operator==(const Partition::PVID& o) const {
    return id == o.id && rangeVersion == o.rangeVersion && assignmentVersion == o.assignmentVersion;
}

bool Partition::PVID::operator!=(const Partition::PVID& o) const {
    return !operator==(o);
}

PartitionGetter::PartitionGetter(Collection&& col) : collection(std::move(col)) {
    if (collection.metadata.hashScheme == HashScheme::Range) {
        _rangePartitionMap.reserve(collection.partitionMap.partitions.size());

        for (auto it = collection.partitionMap.partitions.begin();
                  it != collection.partitionMap.partitions.end();
                  ++it) {
            RangeMapElement e{.key = it->startKey, .partition = &(*it)};
            _rangePartitionMap.push_back(std::move(e));
        }

        std::sort(_rangePartitionMap.begin(), _rangePartitionMap.end());
    }

    if (collection.metadata.hashScheme == HashScheme::HashCRC32C) {
        _hashPartitionMap.reserve(collection.partitionMap.partitions.size());

        for (auto it = collection.partitionMap.partitions.begin();
                  it != collection.partitionMap.partitions.end();
                  ++it) {
            HashMapElement e{.hvalue = std::stoull(it->startKey), .partition = &(*it)};
            _hashPartitionMap.push_back(std::move(e));
        }

        std::sort(_hashPartitionMap.begin(), _hashPartitionMap.end());
    }
}

Partition* PartitionGetter::getPartitionForKey(Key key) {
    switch (collection.metadata.hashScheme) {
        case HashScheme::Range:
        {
            RangeMapElement to_find{.key = std::move(key.partitionKey), .partition = nullptr};
            auto it = std::lower_bound(_rangePartitionMap.begin(), _rangePartitionMap.end(), to_find);
            if (it != _rangePartitionMap.end()) {
                return it->partition;
            }

            return nullptr;
        }
        case HashScheme::HashCRC32C:
        {
            uint32_t c32c = crc32c::Crc32c(key.partitionKey.c_str(), key.partitionKey.size());
            uint64_t hash = c32c;
            // shift the existing hash over to the high 32 bits and add it in to get a 64bit hash
            hash += hash << 32;

            HashMapElement to_find{.hvalue = hash, .partition = nullptr};
            auto it = std::lower_bound(_hashPartitionMap.begin(), _hashPartitionMap.end(), to_find);
            if (it != _hashPartitionMap.end()) {
                return it->partition;
            }

            return nullptr;
        }
        default:
            return nullptr;
    }
}

}  // namespace dto
}  // namespace k2
