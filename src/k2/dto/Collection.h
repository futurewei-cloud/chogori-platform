#pragma once
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <set>
#include <iostream>
#include <unordered_map>
// Collection-related DTOs

namespace k2 {
namespace dto {

// A key for data in K2. We use the partitionKey when determining which partition will own the data, but
// we store the data against a compound key of ${partitionKey}:${rangeKey} to allow user ability to group records
// on the same partition
struct Key {
    // The key used to determine owner partition
    String partitionKey;

    // the range key used to uniquely identify a record in the owner partition
    String rangeKey;
    K2_PAYLOAD_FIELDS(partitionKey, rangeKey);
};

// the assignment state of a partition
enum AssignmentState: uint8_t {
    NotAssigned,
    PendingAssignment,
    Assigned,
    FailedAssignment
};

// Partition in a K2 Collection. By default, the key-range type is String (for range-based partitioning)
// but it can also be an integral type for hash-based partitioning
struct Partition {
    // the partition version
    struct PVersion {
        // the partition id
        uint64_t id = 0;
        // version incremented each time we change the range that this partition owns
        uint64_t rangeVersion = 0;
        // version incremented each time we assign the partition to different K2 node
        uint64_t assignmentVersion = 0;
        K2_PAYLOAD_COPYABLE;
    } pid;
    // the starting key for the partition
    String startKey;
    // the ending key for the partition
    String endKey;
    // the endpoints for the node which is currently assigned to this partition(version)
    std::set<String> endpoints;
    // the current assignment state of the partition
    AssignmentState astate = AssignmentState::NotAssigned;

    K2_PAYLOAD_FIELDS(pid, startKey, endKey, endpoints, astate);

    // Partitions are ordered based on the ordering of their start keys
    bool operator<(const Partition& other) const noexcept {
        return startKey < other.startKey;
    }

    // for debug printing
    friend std::ostream& operator<<(std::ostream& os, const Partition& part) {
        os << "("
           << "rVersion=" << part.pid.rangeVersion
           << ", aVersion=" << part.pid.assignmentVersion
           << ", id=" << part.pid.id
           << ", sKey=" << part.startKey
           << ", eKey=" << part.endKey
           << ", eps=[";
        for (auto& ep: part.endpoints) {
            os << ep << ", ";
        }
        os << "], astate=" << part.astate
        << ")";
        return os;
    }
};

struct PartitionMap {
    uint64_t version =0;
    std::vector<Partition> partitions;
    K2_PAYLOAD_FIELDS(version, partitions);

    // for debug printing
    friend std::ostream& operator<<(std::ostream& os, const PartitionMap& pmap) {
        os << "("
           << "version=" << pmap.version
           << ", partitions=[";
        for (auto& part : pmap.partitions) {
            os << part << " ";
        }
        os << "]";
        return os;
    }
};

struct CollectionCapacity {
    uint64_t dataCapacityMegaBytes = 0;
    uint64_t readIOPs = 0;
    uint64_t writeIOPs = 0;
    K2_PAYLOAD_FIELDS(dataCapacityMegaBytes, readIOPs, writeIOPs);
};

struct CollectionMetadata {
    String name;
    String hashScheme; // e.g. "hash-crc32c"
    String storageDriver; // e.g. "k23si"
    CollectionCapacity capacity;
    K2_PAYLOAD_FIELDS(name, hashScheme, storageDriver, capacity);
};

struct Collection {
    PartitionMap partitionMap;
    std::unordered_map<String, String> userMetadata;
    CollectionMetadata metadata;
    K2_PAYLOAD_FIELDS(partitionMap, userMetadata, metadata);
};

// This is base class for partition getters. Partition getters wrap a Collection object
// and are used to find partitions based on hash/key, depending on the collection type
// Most users should simply use the Wrap builder method
class PartitionGetter {
public:
    // wrap the given collection into a proper PartitionGetter
    static std::unique_ptr<PartitionGetter> Wrap(Collection&& coll);
public:
    PartitionGetter(Collection&& coll);
    virtual ~PartitionGetter();

    // these getters are left without implementation but not declared pure virtual so that
    // code which attempts to use a method not overridden by an implementation will not compile

    // Returns the partition for the given key
    virtual const Partition& getPartitionForKey(Key key);

    // Returns the partition for the given hash value. If the collection doesn't use hashing,
    // the given hvalue is converted to a string and used to find the partition with the range that
    // covers this hvalue
    virtual const Partition& getPartitionForHash(uint64_t hvalue);
    // same as above, but the argument is first converted to uint64_t
    virtual const Partition& getPartitionForHash(String hvalue);

    // the collection we're wrapping
    Collection coll;
}; // class PartitionGetter

// This getter uses crc32c-based hashing to determine key distribution
class HashCRC32CPartitionGetter: public PartitionGetter {
public:
    HashCRC32CPartitionGetter(Collection&& coll);
    virtual ~HashCRC32CPartitionGetter();
    virtual const Partition& getPartitionForKey(Key key) override;
    virtual const Partition& getPartitionForHash(uint64_t hvalue) override;
    virtual const Partition& getPartitionForHash(String hvalue) override;
private:
    std::map<uint64_t, const Partition*> _partitions;
};  // class HashCRC32CPartitionGetter

// This getter uses lexicographical ordering to determine where keys belong
class RangePartitionGetter : public PartitionGetter {
public:
    RangePartitionGetter(Collection&& coll);
    virtual ~RangePartitionGetter();
};  // class RangePartitionGetter

} // namespace dto
} // namespace k2
