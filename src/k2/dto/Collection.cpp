#include "Collection.h"
#include <crc32c/crc32c.h>
#include <string>

namespace k2 {
namespace dto {

std::unique_ptr<PartitionGetter> PartitionGetter::Wrap(Collection&& coll) {
    if (coll.metadata.hashScheme == "hash-crc32") {
        K2DEBUG("Constructing hash-crc32 partition getter for collection: " << coll.metadata.name);
        return std::unique_ptr <PartitionGetter>(new HashCRC32CPartitionGetter(std::move(coll)));
    }
    else if (coll.metadata.hashScheme == "range") {
        K2DEBUG("Constructing range partition getter for collection: " << coll.metadata.name);
        return std::unique_ptr<PartitionGetter>(new RangePartitionGetter(std::move(coll)));
    }
    K2ERROR("Unknown hashing scheme: " << coll.metadata.hashScheme << ", for collection: " << coll.metadata.name);
    return nullptr;
}

PartitionGetter::PartitionGetter(Collection && coll): coll(std::move(coll)){}
PartitionGetter::~PartitionGetter() {}
HashCRC32CPartitionGetter::HashCRC32CPartitionGetter(Collection&& coll): PartitionGetter(std::move(coll)){
    for (auto& part: coll.partitionMap.partitions) {
        // create a vector of pointers to partitions so that we can do binary search faster
        _partitions[std::stoull(part.endKey)] = &part;
    }
}
HashCRC32CPartitionGetter::~HashCRC32CPartitionGetter(){}
RangePartitionGetter::RangePartitionGetter(Collection&& coll) : PartitionGetter(std::move(coll)) {}
RangePartitionGetter::~RangePartitionGetter() {}

const Partition& HashCRC32CPartitionGetter::getPartitionForKey(Key key) {
    uint32_t c32c = crc32c::Crc32c(key.partitionKey.c_str(), key.partitionKey.size());
    uint64_t hash = c32c;
    // shift the existing hash over to the high 32 bits and add it in to get a 64bit hash
    hash += hash << 32;
    return getPartitionForHash(hash);
}

const Partition& HashCRC32CPartitionGetter::getPartitionForHash(uint64_t hvalue) {
    auto it = _partitions.lower_bound(hvalue);
    if (it == _partitions.end()) {
        throw std::runtime_error("invalid partition map - could not find partition for hash value");
    }
    return *(it->second);
}

const Partition& HashCRC32CPartitionGetter::getPartitionForHash(String hvalue) {
    return getPartitionForHash(std::stoull(hvalue.c_str()));
}

}  // namespace dto
}  // namespace k2
