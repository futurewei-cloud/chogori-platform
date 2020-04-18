#include <algorithm>
#include <string>

#include <crc32c/crc32c.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/Discovery.h>

#include "Collection.h"

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
size_t Key::partitionHash() const noexcept {
    uint32_t c32c = crc32c::Crc32c(partitionKey.c_str(), partitionKey.size());
    uint64_t hash = c32c;
    // shift the existing hash over to the high 32 bits and add it in to get a 64bit hash
    hash += hash << 32;
    return hash;
}

bool Partition::PVID::operator==(const Partition::PVID& o) const {
    return id == o.id && rangeVersion == o.rangeVersion && assignmentVersion == o.assignmentVersion;
}

bool Partition::PVID::operator!=(const Partition::PVID& o) const {
    return !operator==(o);
}

PartitionGetter::PartitionWithEndpoint PartitionGetter::GetPartitionWithEndpoint(Partition* p) {
    PartitionWithEndpoint partition{};
    partition.partition = p;
    partition.preferredEndpoint = Discovery::selectBestEndpoint(p->endpoints);

    return partition;
}

PartitionGetter::PartitionGetter(Collection&& col) : collection(std::move(col)) {
    if (collection.metadata.hashScheme == HashScheme::Range) {
        _rangePartitionMap.reserve(collection.partitionMap.partitions.size());

        for (auto it = collection.partitionMap.partitions.begin();
                  it != collection.partitionMap.partitions.end();
                  ++it) {
            RangeMapElement e(it->endKey, GetPartitionWithEndpoint(&(*it)));
            _rangePartitionMap.push_back(std::move(e));
        }

        std::sort(_rangePartitionMap.begin(), _rangePartitionMap.end());
    }

    if (collection.metadata.hashScheme == HashScheme::HashCRC32C) {
        _hashPartitionMap.reserve(collection.partitionMap.partitions.size());

        for (auto it = collection.partitionMap.partitions.begin();
                  it != collection.partitionMap.partitions.end();
                  ++it) {
            HashMapElement e{.hvalue = std::stoull(it->endKey), .partition = GetPartitionWithEndpoint(&(*it))};
            _hashPartitionMap.push_back(std::move(e));
        }

        std::sort(_hashPartitionMap.begin(), _hashPartitionMap.end());
    }
}

PartitionGetter::PartitionWithEndpoint& PartitionGetter::getPartitionForKey(const Key& key) {
    switch (collection.metadata.hashScheme) {
        case HashScheme::Range:
        {
            RangeMapElement to_find(key.partitionKey, PartitionGetter::PartitionWithEndpoint());
            auto it = std::lower_bound(_rangePartitionMap.begin(), _rangePartitionMap.end(), to_find);
            if (it != _rangePartitionMap.end()) {
                return it->partition;
            }

            throw std::runtime_error("no partition for endpoint");
        }
        case HashScheme::HashCRC32C:
        {
            HashMapElement to_find{.hvalue = key.partitionHash(), .partition = PartitionGetter::PartitionWithEndpoint()};
            auto it = std::lower_bound(_hashPartitionMap.begin(), _hashPartitionMap.end(), to_find);
            if (it != _hashPartitionMap.end()) {
                return it->partition;
            }

            throw std::runtime_error("no partition for endpoint");
        }
        default:
            throw std::runtime_error("no partition for endpoint");
    }
}

OwnerPartition::OwnerPartition(Partition&& part, HashScheme scheme) :
    _partition(std::move(part)),
    _scheme(scheme),
    _hstart(std::stoull(_partition.startKey)),
    _hend(std::stoull(_partition.endKey)) {
}

bool OwnerPartition::owns(const Key& key) const {
    switch (_scheme) {
        case HashScheme::Range:
            return _partition.startKey.compare(key.partitionKey) <= 0 && key.partitionKey.compare(_partition.endKey) <=0;
        case HashScheme::HashCRC32C: {
            auto phash = key.partitionHash();
            return _hstart <= phash && phash <= _hend;
        }
        default:
            return false;
    }
}

}  // namespace dto
}  // namespace k2
