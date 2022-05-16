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

#pragma once

#include <common/Chrono.h>
#include <common/Common.h>

#include <set>
#include <iostream>
#include <unordered_map>
#include <functional>

// Collection-related DTOs

namespace skv::http::dto {

// A key for data in K2. We use the partitionKey when determining which partition will own the data, but
// we store the data against a compound key of ${partitionKey}:${rangeKey} to allow user ability to group records
// on the same partition
struct Key {
    // schemaName is needed in the key for uniqueness between records of different schemas
    String schemaName;

    // The key used to determine owner partition
    String partitionKey;

    // the range key used to uniquely identify a record in the owner partition
    String rangeKey;

    int compare(const Key& o) const noexcept;
    bool operator<(const Key& o) const noexcept;
    bool operator<=(const Key& o) const noexcept;
    bool operator>(const Key& o) const noexcept;
    bool operator>=(const Key& o) const noexcept;
    bool operator==(const Key& o) const noexcept;
    bool operator!=(const Key& o) const noexcept;

    // hash useful for hash-containers
    size_t hash() const noexcept;
    // partitioning hash used in K2
    size_t partitionHash() const noexcept;

    K2_PAYLOAD_FIELDS(schemaName, partitionKey, rangeKey);
    K2_DEF_FMT(Key, schemaName, partitionKey, rangeKey);
};

// the assignment state of a partition
K2_DEF_ENUM(AssignmentState,
    NotAssigned,
    PendingAssignment,
    Assigned,
    FailedAssignment
);

// the partition version - used to validate the targeted partition in client requests, by
// ensuring that the client and server have the same understanding of the currently hosted
// partition at a particular K2 node
// This is sent on each and every request to the cluster and so we'd like to keep it as tight as possible
struct PVID {
    // the partition id
    uint64_t id = 0;
    // version incremented each time we change the range that this partition owns
    uint64_t rangeVersion = 0;
    // version incremented each time we assign the partition to different K2 node
    uint64_t assignmentVersion = 0;
    K2_DEF_FMT(PVID, id, rangeVersion, assignmentVersion);

    K2_PAYLOAD_FIELDS(id, rangeVersion, assignmentVersion);

    size_t hash() const noexcept;

    // operators
    bool operator==(const PVID& o) const noexcept;
    bool operator!=(const PVID& o) const noexcept;
};

// A descriptor for the key range of a partition
struct KeyRangeVersion {
    // the starting key for the partition
    String startKey;
    // the ending key for the partition
    String endKey;
    // the partition version
    PVID pvid;

    // hash value
    size_t hash() const noexcept;

    // comparison for unordered containers
    bool operator==(const KeyRangeVersion& o) const noexcept;
    bool operator!=(const KeyRangeVersion& o) const noexcept;
    bool operator<(const KeyRangeVersion& o) const noexcept;

    K2_PAYLOAD_FIELDS(startKey, endKey, pvid);
    K2_DEF_FMT(KeyRangeVersion, startKey, endKey, pvid);
};

// Partition in a K2 Collection. By default, the key-range type is String (for range-based partitioning)
// but it can also be an integral type for hash-based partitioning
struct Partition {
    // the key range version
    KeyRangeVersion keyRangeV;
    // the endpoints for the node which is currently assigned to this partition(version)
    std::set<String> endpoints;
    // the current assignment state of the partition
    AssignmentState astate = AssignmentState::NotAssigned;

    K2_PAYLOAD_FIELDS(keyRangeV, endpoints, astate);
    K2_DEF_FMT(Partition, keyRangeV, endpoints, astate);

    // Partitions are ordered based on the ordering of their start keys
    bool operator<(const Partition& other) const noexcept {
        return keyRangeV < other.keyRangeV;
    }
};

struct PartitionMap {
    uint64_t version =0;
    std::vector<Partition> partitions;
    K2_PAYLOAD_FIELDS(version, partitions);
    K2_DEF_FMT(PartitionMap, version, partitions);
};

struct CollectionCapacity {
    uint64_t dataCapacityMegaBytes = 0;
    uint64_t readIOPs = 0;
    uint64_t writeIOPs = 0;
    uint32_t minNodes = 0;
    K2_PAYLOAD_FIELDS(dataCapacityMegaBytes, readIOPs, writeIOPs, minNodes);
    K2_DEF_FMT(CollectionCapacity, dataCapacityMegaBytes, readIOPs, writeIOPs, minNodes);
};

K2_DEF_ENUM(HashScheme,
    Range,
    HashCRC32C
);

K2_DEF_ENUM(StorageDriver,
            K23SI);


struct CollectionMetadata {
    String name;
    HashScheme hashScheme;
    StorageDriver storageDriver;
    CollectionCapacity capacity;
    Duration retentionPeriod{0};
    Duration heartbeatDeadline{0}; // set by the CPO
    // This is used by the CPO only. If deleted is true the CPO will not return the collection
    // for getCollection RPCs, but the user can try to offload it again.
    bool deleted{false};
    K2_PAYLOAD_FIELDS(name, hashScheme, storageDriver, capacity, retentionPeriod, heartbeatDeadline, deleted);
    K2_DEF_FMT(CollectionMetadata, name, hashScheme, storageDriver, capacity, retentionPeriod, heartbeatDeadline, deleted);
};


struct Collection {
    PartitionMap partitionMap;
    std::unordered_map<String, String> userMetadata;
    CollectionMetadata metadata;

    K2_PAYLOAD_FIELDS(partitionMap, userMetadata, metadata);
    K2_DEF_FMT(Collection, partitionMap, userMetadata, metadata);
};

} // namespace skv::http::dto

// Define std::hash so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<skv::http::dto::Key> {
    size_t operator()(const skv::http::dto::Key& key) const {
        return key.hash();
   }
}; // hash

template <>
struct hash<skv::http::dto::PVID> {
    size_t operator()(const skv::http::dto::PVID& pvid) const {
        return pvid.hash();
    }
}; // hash

template <>
struct hash<skv::http::dto::KeyRangeVersion> {
    size_t operator()(const skv::http::dto::KeyRangeVersion& range) const {
        return range.hash();
    }
};  // hash
} // ns std
