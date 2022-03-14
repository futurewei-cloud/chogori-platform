/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#include <map>
#include <unordered_map>
#include <deque>
#include <optional>

#if K2_MODULE_POOL_ALLOCATOR == 1
// this can only work on GCC > 4
#include <ext/pool_allocator.h>
#endif

#include <k2/common/Common.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/Timestamp.h>

#include "Log.h"

namespace k2 {

// the type holding multiple committed versions of a key
typedef std::deque<dto::DataRecord> VersionsT;

// This struct is the "value" we store for each key in the indexer and represents
// all versions (in MVCC terms) which we have for that key
struct VersionSet {
    // The write intent for the key
    std::optional<dto::WriteIntent> WI;
    // all committed versions, sorted in timestamp-decreasing order
    VersionsT committed;

    // Use to check if there is any data stored in this VSet (WI or committed)
    bool empty() const;

    // This timestamp represents the last time when there was a read for this key. This is used
    // to prevent mutations ot observed history which is required for
    // the consistency guarantees in the K23SI transaction protocol
    dto::Timestamp lastReadTime{dto::Timestamp::ZERO};

    // a static vset used to represent an empty container
    static const VersionSet EMPTY;
};
inline const VersionSet VersionSet::EMPTY{};

// Sorted Key indexer used to map key->vset. It also provides the last observed times at its lowest and highest bounds
struct KeyIndexer {
    // the last time we observed the lowest bound (a virtual key smaller than all other keys)
    dto::Timestamp lastReadTimeLow{dto::Timestamp::ZERO};
    // the last time we observed the highes bound (a virtual key bigger than all other keys)
    dto::Timestamp lastReadTimeHigh{dto::Timestamp::ZERO};
    // the type holding versions for all keys, i.e. the implementation for this key indexer
    #if K2_MODULE_POOL_ALLOCATOR == 1
    typedef std::map<dto::Key, VersionSet, std::less<dto::Key>, __gnu_cxx::__pool_alloc<std::pair<dto::Key, VersionSet>>> KeyIndexerT;
    #else
    typedef std::map<dto::Key, VersionSet> KeyIndexerT;
    #endif
    // the implementation container for storing key->vset
    KeyIndexerT impl;
    // an iterator for our implementation container
    typedef KeyIndexerT::iterator iterator;
};

// The indexer for K2 records. It stores records, mapped as: dto::Key --> dto::DataRecord
class Indexer {
public: // lifecycle
    // The Indexer must be started with a timestamp which indicates when it was created.
    // This timestamp is used to track observations for elements in the indexer.
    seastar::future<> start(dto::Timestamp createdTs);

    // The Indexer must be stopped before it is destroyed to allow for various state to be safely closed.
    seastar::future<> stop();

    // the type for the schema indexer - maps schemaName->KeyIndexer
    typedef std::unordered_map<String, KeyIndexer> SchemaIndexer;

public: // API
    // returns the number of all keys in the indexer
    size_t size();

    // forward declaration for clarity. See class definition further down
    class Iterator;

    // Returns an Iterator for the given key, preset to iterate in the given direction.
    // The iterator only iterates over keys in the same schema as the given key
    Iterator iterate(const dto::Key& key, bool reverse=false);

    // creates a new key indexer for the given schema.
    void createSchema(const dto::Schema& schema);

    // raw access to the underlying schema indexer, used by our debugging APIs
    const SchemaIndexer& getSchemaIndexer() const;

private:
    // the time at which the indexer got created. This will be the assumed observed time for any keys we do not have
    dto::Timestamp _createdTs{dto::Timestamp::ZERO};

    // the indexer, mapping schema_name -> indexer_for_schema
    SchemaIndexer _schemaIndexer;
}; // class KeyIndexer


// This class represents an iterator in the Indexer. Its main features are:
// - allows accessing specific keys
// - supports directional scan operations
// - tracks key or range observations to aid the K23SI transactional consistency
// Iterators are vended by the iterate() API of the indexer
class Indexer::Iterator {
public:
    // A new Iterator is created with iterators for the keys before, at, and after the key for
    // which we created the Iterator.
    // We also need to have a reference to the underlying KeyIndexer which is being iterated
    // as well as the direction of iteration.
    Iterator(KeyIndexer::iterator beforeIt, KeyIndexer::iterator foundIt, KeyIndexer::iterator afterIt, KeyIndexer& si, bool reverse);

public: // APIs
    // returns the time of the last observation(read) on the key associated with the current Iterator position
    dto::Timestamp getLastReadTime() const;

    // returns the time of the last committed value, or ZERO if there are no committed values at the current Iterator position
    dto::Timestamp getLastCommittedTime() const;

    // get the latest DataRecord for the current Iterator position, either WI or committed.
    // Return nullptr if there are no values present (WI or otherwise)
    dto::DataRecord* getLatestDataRecord() const;

    // get the WriteIntent for the current Iterator position
    dto::WriteIntent* getWI() const;

    // get a copy of all data records at the current Iterator position (Debug API)
    std::vector<dto::DataRecord> getAllDataRecords() const;

    // For the current Iterator position, return the correct record for a read done from a transaction
    // with the given timestamp, and a flag indicating if there was a conflict with the existing write intent
    // More formally, get the data record which is either
    // - a write intent from the same transaction (same timestamp), or
    // - committed record, not newer than the given timestamp
    // returns (nullptr, false) if no data record is found at this timestamp and there was no conflicting WI.
    // returns (pointerToWI, true) if a conflicting WI was found at this timestamp.
    std::tuple<dto::DataRecord*, bool> getDataRecordAt(dto::Timestamp ts);

    // Add, abort or commit the WI at the current Iterator position
    void addWI(const dto::Key& key, dto::DataRecord&& rec, uint64_t request_id);
    void abortWI();
    void commitWI();

    // register an observation at the current Iterator position from a transaction with the given timestamp
    void observeAt(dto::Timestamp ts);

    // use to determine if the current Iterator position contains any data records (wi or committed)
    bool empty() const;

    // move on to the next position
    void next();

    // determine if there is a next position to move to.
    bool hasNext() const;

    // If not empty(), returns the key for the current Iterator position. Otherwise the key with empty contents.
    const dto::Key& getKey() const;

private:
    // iterators pointing into the KeyIndexer for the keys:
    // These iterators are always in the forward direction. We may rewind them if we're asked
    // to iterate in reverse but they are always positioned forward.
    KeyIndexer::iterator _beforeIt; // before the current position
    KeyIndexer::iterator _foundIt; // at the current position
    KeyIndexer::iterator _afterIt; // the next position

    // this is the key indexer which is being iterated by this Iterator
    KeyIndexer& _si;

    // the direction in which we're iterating
    bool _reverse{false};
}; // class Iterator

} // namespace k2
