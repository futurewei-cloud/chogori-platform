#include <k2/common/Common.h>
#include <k2/transport/PayloadSerialization.h>
#include <set>
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

// Partition in a K2 Collection. By default, the key-range type is String (for range-based partitioning)
// but it can also be an integral type for hash-based partitioning
struct Partition {
    // version incremented each time we change the range that this partition owns
    uint64_t rangeVersion;
    // version incremented each time we assign the partition to different K2 node
    uint64_t assignmentVersion;
    // the starting key for the partition
    String startKey;
    // the ending key for the partition
    String endKey;
    // the endpoint which is currently assigned to this partition(version)
    String endpoint;

    K2_PAYLOAD_FIELDS(rangeVersion, assignmentVersion, startKey, endKey, endpoint);

    // Partitions are ordered based on the ordering of their start keys
    bool operator<(const Partition& other) const noexcept {
        return startKey < other.startKey;
    }
};

struct PartitionMap {
    uint64_t version;
    std::set<Partition> partitions;
    K2_PAYLOAD_FIELDS(version, partitions);
    Partition& getPartitionForKey(Key key);
};

struct CollectionCapacity {
    uint64_t dataCapacityBytes;
    uint64_t readIOPs;
    uint64_t writeIOPs;
    K2_PAYLOAD_FIELDS(dataCapacityBytes, readIOPs, writeIOPs);
};

struct CollectionMetadata {
    String name;
    String hashScheme;
    String storageDriver;
    CollectionCapacity capacity;
    K2_PAYLOAD_FIELDS(name, hashScheme, storageDriver, capacity);
};

struct Collection {
    PartitionMap partitionMap;
    std::unordered_map<String, String> userMetadata;
    CollectionMetadata metadata;
    K2_PAYLOAD_FIELDS(partitionMap, userMetadata, metadata);
};

} // namespace dto
} // namespace k2
