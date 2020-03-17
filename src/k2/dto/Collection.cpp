#include <algorithm>
#include <string>

#include <crc32c/crc32c.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RRDMARPCProtocol.h>

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

bool Partition::PVID::operator==(const Partition::PVID& o) const {
    return id == o.id && rangeVersion == o.rangeVersion && assignmentVersion == o.assignmentVersion;
}

bool Partition::PVID::operator!=(const Partition::PVID& o) const {
    return !operator==(o);
}

PartitionGetter::PartitionWithEndpoint PartitionGetter::GetPartitionWithEndpoint(Partition* p) {
    PartitionWithEndpoint partition{};
    partition.partition = p;

    for (auto ep = p->endpoints.begin(); ep != p->endpoints.end(); ++ep) {
        partition.preferredEndpoint = *(RPC().getTXEndpoint(*ep));

        if (seastar::engine()._rdma_stack && 
                partition.preferredEndpoint.getProtocol() == RRDMARPCProtocol::proto) {
            break;
        }
    }

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

PartitionGetter::PartitionWithEndpoint PartitionGetter::getPartitionForKey(const Key& key) {
    switch (collection.metadata.hashScheme) {
        case HashScheme::Range:
        {
            RangeMapElement to_find(key.partitionKey, PartitionGetter::PartitionWithEndpoint());
            auto it = std::lower_bound(_rangePartitionMap.begin(), _rangePartitionMap.end(), to_find);
            if (it != _rangePartitionMap.end()) {
                return it->partition;
            }

            return PartitionWithEndpoint{};
        }
        case HashScheme::HashCRC32C:
        {
            uint32_t c32c = crc32c::Crc32c(key.partitionKey.c_str(), key.partitionKey.size());
            uint64_t hash = c32c;
            // shift the existing hash over to the high 32 bits and add it in to get a 64bit hash
            hash += hash << 32;

            HashMapElement to_find{.hvalue = hash, .partition = PartitionGetter::PartitionWithEndpoint()};
            auto it = std::lower_bound(_hashPartitionMap.begin(), _hashPartitionMap.end(), to_find);
            if (it != _hashPartitionMap.end()) {
                return it->partition;
            }

            return PartitionWithEndpoint{};
        }
        default:
            return PartitionWithEndpoint{};
    }
}

}  // namespace dto
}  // namespace k2
