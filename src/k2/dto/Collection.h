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

#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <k2/transport/TXEndpoint.h>

#include <set>
#include <iostream>
#include <unordered_map>
#include <functional>

#include <nlohmann/json.hpp>

// Collection-related DTOs

namespace k2 {
namespace dto {

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

    // hash useful for hash-containers
    size_t hash() const noexcept;
    // partitioning hash used in K2
    size_t partitionHash() const noexcept;

    K2_PAYLOAD_FIELDS(schemaName, partitionKey, rangeKey);

    friend std::ostream& operator<<(std::ostream& os, const Key& key) {
        return os << "{schema=" << key.schemaName << " pkey=" << key.partitionKey << ", rkey=" << key.rangeKey << "}";
    }
};

void inline to_json(nlohmann::json& j, const Key& key) {
    j = nlohmann::json{{"schemaName", key.schemaName}, {"partitionKey", key.partitionKey}, 
                       {"rangeKey", key.rangeKey}};
}

void inline from_json(const nlohmann::json& j, Key& key) {
    j.at("schemaName").get_to(key.schemaName);
    j.at("partitionKey").get_to(key.partitionKey);
    j.at("rangeKey").get_to(key.rangeKey);
}

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
    return os << strstate;
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
        friend std::ostream& operator<<(std::ostream& os, const PVID& pvid) {
            return os << "{id=" << pvid.id << ", rangeV=" << pvid.rangeVersion << ", assignmentV=" << pvid.assignmentVersion << "}";
        }

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
        os << "{"
           << "rVersion=" << part.pvid.rangeVersion
           << ", aVersion=" << part.pvid.assignmentVersion
           << ", id=" << part.pvid.id
           << ", sKey=" << part.startKey
           << ", eKey=" << part.endKey
           << ", eps=[";
        for (auto& ep: part.endpoints) {
            os << ep << ", ";
        }
        os << "], astate=" << part.astate << "}";
        return os;
    }
};

// TODO additional fields
void inline to_json(nlohmann::json& j, const Partition& Partition) {
    j = nlohmann::json{{"startKey", Partition.startKey}, 
                       {"endKey", Partition.endKey}, 
                       {"endpoints", Partition.endpoints}, 
                       {"astate", Partition.astate}};
}

void inline from_json(const nlohmann::json& j, Partition& Partition) {
    j.at("startKey").get_to(Partition.startKey);
    j.at("endKey").get_to(Partition.endKey);
    j.at("endpoints").get_to(Partition.endpoints);
    j.at("astate").get_to(Partition.astate);
}

struct PartitionMap {
    uint64_t version =0;
    std::vector<Partition> partitions;
    K2_PAYLOAD_FIELDS(version, partitions);

    // for debug printing
    friend std::ostream& operator<<(std::ostream& os, const PartitionMap& pmap) {
        os << "{"
           << "version=" << pmap.version
           << ", partitions=[";
        for (auto& part : pmap.partitions) {
            os << part << " ";
        }
        os << "]}";
        return os;
    }
};

void inline to_json(nlohmann::json& j, const PartitionMap& map) {
    j = nlohmann::json{{"version", map.version}, 
                       {"partitions", map.partitions}};
}

void inline from_json(const nlohmann::json& j, PartitionMap& map) {
    j.at("version").get_to(map.version);
    j.at("partitions").get_to(map.partitions);
}

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

// TODO additional fields
void inline to_json(nlohmann::json& j, const CollectionMetadata& meta) {
    j = nlohmann::json{{"name", meta.name}, 
                       {"hashScheme", meta.hashScheme}};
}

void inline from_json(const nlohmann::json& j, CollectionMetadata& meta) {
    j.at("name").get_to(meta.name);
    j.at("hashScheme").get_to(meta.hashScheme);
}

struct Collection {
    PartitionMap partitionMap;
    std::unordered_map<String, String> userMetadata;
    CollectionMetadata metadata;

    K2_PAYLOAD_FIELDS(partitionMap, userMetadata, metadata);
};

void inline to_json(nlohmann::json& j, const Collection& collection) {
    j = nlohmann::json{{"partitionMap", collection.partitionMap}, 
                       {"metadata", collection.metadata}};
}

void inline from_json(const nlohmann::json& j, Collection& collection) {
    j.at("partitionMap").get_to(collection.partitionMap);
    j.at("metadata").get_to(collection.metadata);
}

class PartitionGetter {
public:
    struct PartitionWithEndpoint {
        Partition* partition;
        std::unique_ptr<TXEndpoint> preferredEndpoint;
        friend std::ostream& operator<<(std::ostream& os, const PartitionWithEndpoint& pwe) {
            os << "partition: ";
            if (!pwe.partition) {
                os << "(null)";
            }
            else {
                os << *pwe.partition;
            }
            os << ", preferred endpoint: ";
            if (!pwe.preferredEndpoint) {
                os << "(null)";
            }
            else {
                os << *pwe.preferredEndpoint;
            }

            return os;
        }
    };

    PartitionGetter(Collection&& collection);
    PartitionGetter() = default;

    // Returns the partition and preferred endpointfor the given key.
    // Hashes key if hashScheme is not range
    PartitionWithEndpoint& getPartitionForKey(const Key& key, bool reverse=false, bool exclusiveKey=false);

    Collection collection;

private:
    static PartitionWithEndpoint GetPartitionWithEndpoint(Partition* p);

    struct RangeMapElement {
        RangeMapElement(const String& k, PartitionWithEndpoint part) : key(k), partition(std::move(part)) {}

        std::reference_wrapper<const String> key;
        PartitionWithEndpoint partition;

        bool operator<(const RangeMapElement& other) const noexcept {
            return key.get() < other.key.get();
        }
    };

    struct HashMapElement {
        uint64_t hvalue;
        PartitionWithEndpoint partition;
        bool operator<(const HashMapElement& other) const noexcept {
            return hvalue < other.hvalue;
        }
    };

    std::vector<RangeMapElement> _rangePartitionMap;
    std::vector<HashMapElement> _hashPartitionMap;
};

// Helper wrapper for Partitions, which allows to establish
// if a partition owns a key
class OwnerPartition {
public:
    OwnerPartition(Partition&& part, HashScheme scheme);
    bool owns(const Key& key, const bool reverse = false) const;
    Partition& operator()() { return _partition; }
    const Partition& operator()() const { return _partition; }
    HashScheme getHashScheme() { return _scheme; }
    friend std::ostream& operator<<(std::ostream& os, const OwnerPartition& p) {
        return os << p._partition;
    }


private:
    Partition _partition;
    HashScheme _scheme;
    uint64_t _hstart;
    uint64_t _hend;

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
