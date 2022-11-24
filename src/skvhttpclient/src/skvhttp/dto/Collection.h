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

#include <skvhttp/common/Common.h>

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

    K2_SERIALIZABLE(Key, schemaName, partitionKey, rangeKey);

	friend std::ostream& operator<<(std::ostream& os, const Key& key) {
        return os << fmt::format(
            "{{schemaName={}, partitionKey={}, rangeKey={}}}",
			key.schemaName,
			HexCodec::encode(key.partitionKey),
			HexCodec::encode(key.rangeKey));
    }
};


struct CollectionCapacity {
    uint64_t dataCapacityMegaBytes = 0;
    uint64_t readIOPs = 0;
    uint64_t writeIOPs = 0;
    uint32_t minNodes = 0;
    K2_SERIALIZABLE_FMT(CollectionCapacity, dataCapacityMegaBytes, readIOPs, writeIOPs, minNodes);
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
    K2_SERIALIZABLE_FMT(CollectionMetadata, name, hashScheme, storageDriver, capacity, retentionPeriod, heartbeatDeadline, deleted);
};

} // namespace skv::http::dto

// Define std::hash so that we can use them in hash maps/sets
namespace std {
template <>
struct hash<skv::http::dto::Key> {
    size_t operator()(const skv::http::dto::Key& key) const {
        return key.hash();
    }
};  // hash
} // ns std
