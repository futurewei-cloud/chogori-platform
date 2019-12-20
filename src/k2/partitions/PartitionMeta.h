#pragma once
#include <k2/common/Common.h>
#include <map>

namespace k2 {

struct PartitionID {
    uint64_t id;
    uint64_t assignmentVersion;
    uint64_t rangeVersion;
};

struct PartitionRange {
    String from;
    String to;
};

struct Partition {
    PartitionID id;
    PartitionRange range;
    String endpoint;
};

struct PartitionMap {
    uint64_t version;  // incremented if any partition is modified
    std::map<String, Partition*> partitions; // map of low_key to partition which owns the range that starts with that low key
    const Partition* getPartitionForKey(const String& key) const;
};

} // namespace k2
