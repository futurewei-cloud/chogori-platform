#include "Indexer.h"

#include <k2/common/MapUtil.h>

#include <iterator>

namespace k2 {

// *********************** VersionSet API
bool VersionSet::empty() const {
    return !WI.has_value() && committed.empty();
}
// *********************** end VersionSet API

// *********************** Indexer API
seastar::future<> Indexer::start(dto::Timestamp createdTs) {
    _createdTs = createdTs;
    return seastar::make_ready_future();
}

seastar::future<> Indexer::stop() {
    return seastar::make_ready_future();
}

const Indexer::SchemaIndexer& Indexer::getSchemaIndexer() const {
    return _schemaIndexer;
}

void Indexer::createSchema(const dto::Schema& schema) {
    // create a default indexer for the schema if one doesn't exist
    auto [iter, success] = _schemaIndexer.try_emplace(schema.name);
    if (success) {
        K2LOG_D(log::skvsvr, "Created new schema indexer for {}", schema.name);
        // if we did create a new indexer, set the low/high watermarks to the time we created the main indexer
        iter->second.lastReadTimeLow = _createdTs;
        iter->second.lastReadTimeHigh = _createdTs;
    }
}

size_t Indexer::size() {
    // NB, this is not O(1) as we could make it, but in practice it may not matter much
    // We should also report key count per schema as a metric, which would mean iterating over
    // the schema indexes anyway.
    size_t sz = 0;
    for (auto&[_,idxr]: _schemaIndexer) {
        sz += idxr.impl.size();
    }
    return sz;
}

Indexer::Iterator Indexer::iterate(const dto::Key& key, bool reverse) {
    // if schema doesn't exist, it is an internal error - upon deployment of a new schema, we create an indexer for it
    auto it = _schemaIndexer.find(key.schemaName);
    if (it == _schemaIndexer.end()) {
        K2LOG_E(log::skvsvr, "Invalid schema in key: {}", key);
        throw std::runtime_error("Schema does not exist in schema indexer");
    }

    auto [before, found, after] = keyRange(key, it->second.impl);

    return Iterator(before, found, after, it->second, reverse);
}
// *********************** end Indexer API

// *********************** Indexer::Iterator API
Indexer::Iterator::Iterator(KeyIndexer::iterator beforeIt, KeyIndexer::iterator foundIt, KeyIndexer::iterator afterIt, KeyIndexer& si, bool reverse):
    _beforeIt(beforeIt), _foundIt(foundIt), _afterIt(afterIt), _si(si), _reverse(reverse) {
}

// returns the time of the last observation(read) on the key associated with this Iterator.
dto::Timestamp Indexer::Iterator::getLastReadTime() const {
    // 1. this Iterator points to a an existing key. Return the stored ts
    if (_foundIt != _si.impl.end()) {
        return _foundIt->second.lastReadTime;
    }
    // 2. this Iterator points to a non-existing key. Return the min(neighborLow, neighborHigh).
    auto nlow = _beforeIt == _si.impl.end() ? _si.lastReadTimeLow : _beforeIt->second.lastReadTime;
    auto nhigh = _afterIt == _si.impl.end() ? _si.lastReadTimeHigh : _afterIt->second.lastReadTime;
    return nlow.min(nhigh);
}

// returns the time of the last committed value, or ZERO if there are no committed values
dto::Timestamp Indexer::Iterator::getLastCommittedTime() const {
    if (_foundIt != _si.impl.end() && _foundIt->second.committed.size() > 0) {
        return _foundIt->second.committed[0].timestamp;
    }
    return dto::Timestamp::ZERO;
}

// get the latest DataRecord in this Iterator, either WI or committed.
// Return nullptr if there are no values present (WI or otherwise)
dto::DataRecord* Indexer::Iterator::getLatestDataRecord() const {
    if (_foundIt == _si.impl.end()) {
        return nullptr;
    }
    if (_foundIt->second.WI.has_value()) {
        return &(_foundIt->second.WI->data);
    } else if (_foundIt->second.committed.size() > 0) {
        return &(_foundIt->second.committed[0]);
    }
    return nullptr;
}

dto::WriteIntent* Indexer::Iterator::getWI() const {
    if (_foundIt != _si.impl.end() && _foundIt->second.WI.has_value()) {
        return &(_foundIt->second.WI.value());
    }
    return nullptr;
}

std::vector<dto::DataRecord> Indexer::Iterator::getAllDataRecords() const {
    std::vector<dto::DataRecord> result;
    if (_foundIt == _si.impl.end()) {
        return result;
    }
    result.reserve(1 + _foundIt->second.committed.size());
    auto* wi = getWI();
    if (wi) {
        dto::DataRecord copy{
            .value = wi->data.value.share(),
            .timestamp = wi->data.timestamp,
            .isTombstone = wi->data.isTombstone};

        result.push_back(std::move(copy));
    }

    for (auto& rec : _foundIt->second.committed) {
        dto::DataRecord copy{
            .value = rec.value.share(),
            .timestamp = rec.timestamp,
            .isTombstone = rec.isTombstone};

        result.push_back(std::move(copy));
    }
    return result;
}

std::tuple<dto::DataRecord*, bool> Indexer::Iterator::getDataRecordAt(dto::Timestamp ts) {
    if (_foundIt == _si.impl.end()) {
        return std::make_tuple(nullptr, false);
    }
    if (auto* wi = getWI(); wi) {
        auto comp = wi->data.timestamp.compareCertain(ts);
        if (comp == dto::Timestamp::EQ) {
            // WI is from same transaction
            return std::make_tuple(&(wi->data), false);
        }
        if (comp == dto::Timestamp::LT) {
            // WI is older than ts which means a conflicting transaction
            return std::make_tuple(&(wi->data), true);
        }
        // we have a WI but it is newer than the timestamp - fallthrough
    }

    // return the first record we can find which is older than the given timestamp
    for (auto& rec: _foundIt->second.committed) {
        if (rec.timestamp.compareCertain(ts) <= 0) {
            return std::make_tuple(&rec, false);
        }
    }
    return std::make_tuple(nullptr, false);
}

void Indexer::Iterator::addWI(const dto::Key& key, dto::DataRecord&& rec, uint64_t request_id) {
    // We need to create the key in the indexer if it didn't exist before
    if (_foundIt == _si.impl.end()) {
        auto lastRead = getLastReadTime();
        _foundIt = _si.impl.insert(_afterIt, std::pair<dto::Key, VersionSet>(key, VersionSet{}));
        // mark the new entry as being observed at the same time as the neighbors.
        _foundIt->second.lastReadTime = lastRead;
        K2LOG_D(log::skvsvr, "Created new key {}", key);
    }
    else {
        K2ASSERT(log::skvsvr, getKey() == key, "Key mismatch while adding key: have={}, given={}", getKey(), key);
    }
    K2ASSERT(log::skvsvr, !_foundIt->second.WI.has_value(), "Unable to insert WI due to previous WI present: {}", _foundIt->second.WI.value());
    _foundIt->second.WI.emplace(std::move(rec), request_id);
}

void Indexer::Iterator::abortWI() {
    if (_foundIt != _si.impl.end()) {
        _foundIt->second.WI.reset();
        if (_foundIt->second.committed.empty()) {
            auto lastObservedAt = _foundIt->second.lastReadTime;
            // this entire entry can now be removed as it has no WI and no committed data
            K2LOG_D(log::skvsvr, "Removing empty entry for key={}, lastObserved=", getKey(), lastObservedAt);
            _si.impl.erase(_foundIt);
            _foundIt = _si.impl.end();
            // update the neighbors with our timestamp
            observeAt(lastObservedAt);
        }
    }
}

void Indexer::Iterator::commitWI() {
    K2ASSERT(log::skvsvr, _foundIt != _si.impl.end() && _foundIt->second.WI.has_value(), "WI must have value to commit");
    _foundIt->second.committed.push_front(std::move(_foundIt->second.WI->data));
    _foundIt->second.WI.reset();
}

void Indexer::Iterator::observeAt(dto::Timestamp ts) {
    K2LOG_D(log::skvsvr, "Observing at ts={}, bit={}, fit={}, ait={}", ts,
            _beforeIt == _si.impl.end() ? dto::Key{} : _beforeIt->first,
            _foundIt == _si.impl.end() ? dto::Key{} : _foundIt->first,
            _afterIt == _si.impl.end() ? dto::Key{} : _afterIt->first);
    if (_foundIt != _si.impl.end()) {
        _foundIt->second.lastReadTime.maxEq(ts);
    } else {
        _beforeIt != _si.impl.end() ? _beforeIt->second.lastReadTime.maxEq(ts) : _si.lastReadTimeLow.maxEq(ts);
        _afterIt  != _si.impl.end() ? _afterIt->second.lastReadTime.maxEq(ts)  : _si.lastReadTimeHigh.maxEq(ts);
    }
}

bool Indexer::Iterator::empty() const {
    return _foundIt == _si.impl.end() || _foundIt->second.empty();
}

void Indexer::Iterator::next() {
    if (!_reverse && _afterIt != _si.impl.end()) {
        // forward direction
        if (_foundIt != _si.impl.end()) {
            // only move our _beforeIt if _foundIt actually was pointing to an element.
            _beforeIt = _foundIt;
        }
        _foundIt = _afterIt;
        ++_afterIt;
    }
    else if (_reverse && _beforeIt != _si.impl.end()) {
        // reverse direction
        if (_foundIt != _si.impl.end()) {
            // only move our _afterIt if _foundIt actually was pointing to an element.
            _afterIt = _foundIt;
        }
        _foundIt = _beforeIt;
        if (_beforeIt != _si.impl.begin()) {
            // this is only safe to do if we are not at begin()
            --_beforeIt;
        } else {
            // we can't rewind begin. Set it to end() directly
            _beforeIt = _si.impl.end();
        }
    }
}

bool Indexer::Iterator::hasNext() const {
    return (_reverse && _beforeIt != _si.impl.end()) ||
           (!_reverse && _afterIt != _si.impl.end());
}

const dto::Key& Indexer::Iterator::getKey() const {
    K2ASSERT(log::skvsvr, _foundIt != _si.impl.end(), "getKey invoked on an empty iterator");
    return _foundIt->first;
}
// *********************** end Indexer::Iterator API

} // ns k2