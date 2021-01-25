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

#include <algorithm>
#include <list>
#include <iostream>

#include <intervaltree.hpp>

using namespace Intervals;

template <typename KeyT, typename TimestampT>
class ReadCache
{
public:
    ReadCache(TimestampT min_timestamp, size_t cache_size) : _min(min_timestamp), _max_size(cache_size) {}

    TimestampT checkInterval(KeyT low, KeyT high)
    {
        auto interval = Interval<KeyT, TreeValue>(std::move(low), std::move(high));
        auto overlapping = _tree.findOverlappingIntervals(interval);
        if (overlapping.size() == 0) {
            return _min;
        }

        TimestampT most_recent = _min;
        for (auto it = overlapping.begin(); it != overlapping.end(); ++it) {
            most_recent = do_max(most_recent, it->value.timestamp);
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
            found->value.timestamp = do_max(timestamp, found->value.timestamp);
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
            _min = do_max(_min, to_remove.value);
            _lru.pop_back();
        }
    }

private:
    static TimestampT do_max(const TimestampT& x, const TimestampT& y)
    {
        using std::max;
        return max(x, y);
    }

    struct TreeValue {
        typename std::list<Interval<KeyT, TimestampT>>::iterator it;
        TimestampT timestamp;
    };

    IntervalTree<KeyT, TreeValue> _tree;
    std::list<Interval<KeyT, TimestampT>> _lru;
    TimestampT _min;
    size_t _max_size;
};
