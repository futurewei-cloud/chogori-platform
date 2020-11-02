/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include <algorithm>
#include <string>

#include <crc32c/crc32c.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/Discovery.h>

#include "Collection.h"

namespace k2 {
namespace dto {

int Key::compare(const Key& o) const noexcept {
    auto scomp = schemaName.compare(o.schemaName);
    if (scomp != 0) {
        return scomp;
    }

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
    return std::hash<k2::String>()(schemaName) + std::hash<k2::String>()(partitionKey) + std::hash<k2::String>()(rangeKey);
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
            // For range partitioning, we need to use the startKey because the last
            // range end key will be an empty string ("") and wouldn't be sorted correctly
            RangeMapElement e(it->startKey, GetPartitionWithEndpoint(&(*it)));
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

PartitionGetter::PartitionWithEndpoint& PartitionGetter::getPartitionForKey(const Key& key, bool reverse, bool exclusiveKey) {
    switch (collection.metadata.hashScheme) {
        case HashScheme::Range:
        {
            RangeMapElement to_find(key.partitionKey, PartitionGetter::PartitionWithEndpoint());

            // case 1: if get partiton in the reverse direction, and the key is empty: return the last partition;
            //         empty key in the forward direction can be well treated by upper_bound (case 3).
            // case 2: if exclusiveKey is true, use lower_bound to get partition; 
            //         forward direction with true_exclusiveKey and empry_key is not allow, throw exception.
            // case 3: if exclusiveKey is false, use upper_bound to get partition.
            std::vector<RangeMapElement>::iterator it;
            if (reverse && key.partitionKey == "") {
                return _rangePartitionMap.rbegin()->partition;
            } else if (exclusiveKey) {
                // if the 'exclusiveKey' is true (start keys are exclusive), lower_bound gives the start key,
                // so we return the partition before the one that obtained by lower_bound.
                it = std::lower_bound(_rangePartitionMap.begin(), _rangePartitionMap.end(), to_find);
                if (it == _rangePartitionMap.begin()) {
                    throw std::runtime_error("forward direction with empry_key and true_exclusiveKey is not allowed!");
                }
            } else {
                // We are comparing against the start keys and upper_bound gives the first start key
                // greater than the key (start keys are inclusive), so we return the partition before 
                // the one obtained by upper_bound
                it = std::upper_bound(_rangePartitionMap.begin(), _rangePartitionMap.end(), to_find);
                K2ASSERT(it != _rangePartitionMap.begin(), "Partition map does not begin with an empty string start key!");
            }

            if (it != _rangePartitionMap.end()) {
                return (--it)->partition;
            }

            return _rangePartitionMap.rbegin()->partition;
        }
        case HashScheme::HashCRC32C:
        {
            HashMapElement to_find{.hvalue = key.partitionHash(), .partition = PartitionGetter::PartitionWithEndpoint()};
            auto it = std::upper_bound(_hashPartitionMap.begin(), _hashPartitionMap.end(), to_find);
            if (it != _hashPartitionMap.end()) {
                return it->partition;
            }

            throw std::runtime_error("no partition for endpoint");
        }
        default:
            throw std::runtime_error("Unknown hash scheme for collection");
    }
}

OwnerPartition::OwnerPartition(Partition&& part, HashScheme scheme) :
    _partition(std::move(part)),
    _scheme(scheme) {

    if (_scheme == HashScheme::HashCRC32C) {
        _hstart = std::stoull(_partition.startKey);
        _hend = std::stoull(_partition.endKey);
    }
}

bool OwnerPartition::owns(const Key& key, const bool reverse) const {
    switch (_scheme) {
        case HashScheme::Range:
            if (!reverse) {
                if (_partition.endKey == "") {
                    return _partition.startKey.compare(key.partitionKey) <= 0;
                }
                
                return _partition.startKey.compare(key.partitionKey) <= 0 && key.partitionKey.compare(_partition.endKey) < 0;
            } else {
                if (key.partitionKey == "" && _partition.endKey == "") return true;
                else if (key.partitionKey == "" && _partition.endKey != "") return false;
                else if (_partition.endKey == "") return _partition.startKey.compare(key.partitionKey) <= 0;
                
                return _partition.startKey.compare(key.partitionKey) <= 0 && key.partitionKey.compare(_partition.endKey) <= 0;
            }
        case HashScheme::HashCRC32C: {
            auto phash = key.partitionHash();
            return _hstart <= phash && phash < _hend;
        }
        default:
            return false;
    }
}

}  // namespace dto
}  // namespace k2
