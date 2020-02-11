/*
*   (C)opyright Futurewei Technologies Inc, 2020
*/

#pragma once

#include <algorithm>
#include <list>
#include <iostream>

#include <k2/module/k23si/IntervalTree/include/intervaltree.hpp>

using namespace Intervals;

template <typename KeyT, typename TimestampT>
class ReadCache
{
public:
    ReadCache(TimestampT min_timestamp, size_t cache_size=10000) : _min(min_timestamp), _max_size(cache_size) {}

    TimestampT checkInterval(KeyT low, KeyT high)
    {
        auto interval = Interval<KeyT, TreeValue>(std::move(low), std::move(high));
        auto overlapping = _tree.findOverlappingIntervals(interval);
        if (overlapping.size() == 0) {
            return _min;
        }

        TimestampT most_recent = _min;
        for (auto it = overlapping.begin(); it != overlapping.end(); ++it) {
            most_recent = std::max(most_recent, it->value.timestamp);
        }

        return most_recent;
    }

    void insertInterval(KeyT low, KeyT high, TimestampT timestamp)
    {
        Interval<KeyT, TreeValue>* found = _tree.find(Interval<KeyT, TreeValue>(low, high));
        if (found) {
            _lru.erase(found->value.it);
            _lru.emplace_front(std::move(low), std::move(high), timestamp);
            found->value.it = _lru.begin();
            found->value.timestamp = std::max(timestamp, found->value.timestamp);
            return;
        }

        _lru.emplace_front(low, high, timestamp);
        Interval<KeyT, TreeValue> interval = Interval<KeyT, TreeValue>(std::move(low), std::move(high));
        interval.value.timestamp = timestamp;
        interval.value.it = _lru.begin();
        _tree.insert(std::move(interval));

        if (_lru.size() > _max_size) {
            Interval<KeyT, TimestampT>& to_remove = _lru.back();
            _tree.remove(Interval<KeyT, TreeValue>(to_remove.low, to_remove.high));
            _min = std::max(_min, to_remove.value);
            _lru.pop_back();
        }
    }

private:
    struct TreeValue {
        typename std::list<Interval<KeyT, TimestampT>>::iterator it;
        TimestampT timestamp;
    };

    IntervalTree<KeyT, TreeValue> _tree;
    std::list<Interval<KeyT, TimestampT>> _lru;
    TimestampT _min;
    size_t _max_size;
};
