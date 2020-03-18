#pragma once
#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <set>
#include <iostream>
#include <unordered_map>
#include <functional>
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

    int compare(const Key& o) const noexcept;
    bool operator<(const Key& o) const noexcept;
    bool operator<=(const Key& o) const noexcept;
    bool operator>(const Key& o) const noexcept;
    bool operator>=(const Key& o) const noexcept;
    bool operator==(const Key& o) const noexcept;

    size_t hash() const noexcept;
    K2_PAYLOAD_FIELDS(partitionKey, rangeKey);

    friend std::ostream& operator<<(std::ostream& os, const Key& key) {
        return os << "pkey=" << key.partitionKey << ", rkey=" << key.rangeKey;
    }
};

// the assignment state of a partition
enum class AssignmentState: uint8_t {
    NotAssigned,
    PendingAssignment,
    Assigned,
    FailedAssignment
};

inline std::ostream& operator<<(std::ostream& os, const AssignmentState& state) {
    const char* strstate = "bad state";
    switch (state) {
        case AssignmentState::NotAssigned: strstate= "not_assigned"; break;
        case AssignmentState::PendingAssignment: strstate= "pending_assignment"; break;
        case AssignmentState::Assigned: strstate= "assigned"; break;
        case AssignmentState::FailedAssignment: strstate= "failed_assignment"; break;
        default: break;
    }
    return os << "state=" << strstate;
}

// Partition in a K2 Collection. By default, the key-range type is String (for range-based partitioning)
// but it can also be an integral type for hash-based partitioning
struct Partition {
    // the partition version
    struct PVID {
        // the partition id
        uint64_t id = 0;
        // version incremented each time we change the range that this partition owns
        uint64_t rangeVersion = 0;
        // version incremented each time we assign the partition to different K2 node
        uint64_t assignmentVersion = 0;
        K2_PAYLOAD_COPYABLE;

        // operators
        bool operator==(const PVID& o) const;
        bool operator!=(const PVID& o) const;
    } pvid;
    // the starting key for the partition
    String startKey;
    // the ending key for the partition
    String endKey;
    // the endpoints for the node which is currently assigned to this partition(version)
    std::set<String> endpoints;
    // the current assignment state of the partition
    AssignmentState astate = AssignmentState::NotAssigned;

    K2_PAYLOAD_FIELDS(pvid, startKey, endKey, endpoints, astate);

    // Partitions are ordered based on the ordering of their start keys
    bool operator<(const Partition& other) const noexcept {
        return startKey < other.startKey;
    }

    // for debug printing
    friend std::ostream& operator<<(std::ostream& os, const Partition& part) {
        os << "("
           << "rVersion=" << part.pvid.rangeVersion
           << ", aVersion=" << part.pvid.assignmentVersion
           << ", id=" << part.pvid.id
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

enum struct HashScheme {
    Range,
    HashCRC32C
};

inline std::ostream& operator<<(std::ostream& os, const HashScheme& scheme) {
    switch (scheme) {
        case HashScheme::Range:
            return os << "Range";
        case HashScheme::HashCRC32C:
            return os << "Hash-CRC32C";
        default:
            return os << "Unknown hash scheme";
    }
}

enum struct StorageDriver {
    K23SI
};

inline std::ostream& operator<<(std::ostream& os, const StorageDriver& driver) {
    switch (driver) {
        case StorageDriver::K23SI:
            return os << "K23SI";
        default:
            return os << "Unknown storage driver";
    }
}

struct CollectionMetadata {
    String name;
    HashScheme hashScheme;
    StorageDriver storageDriver;
    CollectionCapacity capacity;
    Duration retentionPeriod{0};
    Duration heartbeatDeadline{0}; // set by the CPO
    K2_PAYLOAD_FIELDS(name, hashScheme, storageDriver, capacity, retentionPeriod, heartbeatDeadline);
};

struct Collection {
    PartitionMap partitionMap;
    std::unordered_map<String, String> userMetadata;
    CollectionMetadata metadata;

    K2_PAYLOAD_FIELDS(partitionMap, userMetadata, metadata);
};

class PartitionGetter {
public:
    PartitionGetter(Collection&& collection);
    PartitionGetter() = default;

    // Returns the partition for the given key. Hashes key if hashScheme is not range
    Partition* getPartitionForKey(const Key& key);

    Collection collection;

private:
    struct RangeMapElement {
        RangeMapElement(const String& k, Partition* part) : key(k), partition(part) {}

        std::reference_wrapper<const String> key;
        Partition* partition;

        bool operator<(const RangeMapElement& other) const noexcept {
            return key.get() < other.key.get();
        }
    };

    struct HashMapElement {
        uint64_t hvalue;
        Partition* partition;
        bool operator<(const HashMapElement& other) const noexcept {
            return hvalue < other.hvalue;
        }
    };

    std::vector<RangeMapElement> _rangePartitionMap;
    std::vector<HashMapElement> _hashPartitionMap;
};

} // namespace dto
} // namespace k2

// Define std::hash for Keys so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<k2::dto::Key> {
    size_t operator()(const k2::dto::Key& key) const {
        return key.hash();
   }
}; // hash
} // ns std
