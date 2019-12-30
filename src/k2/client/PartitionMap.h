#pragma once

#include <cstdint>
#include "IClient.h"
#include <atomic>
#include <set>

namespace k2
{

namespace client
{

static bool intersectsRangeHighPartitionLow(const Range& range, const PartitionRange& partition)
{
    const String& rangeHighKey = range.isSingleKey() ? range.getLowKey() : range.getHighKey();

    if(partition.lowKey.empty() || rangeHighKey.empty()) {
        return true;
    }

    if(range.isHighKeyInclusive()) {
        return rangeHighKey.compare(partition.lowKey) >= 0;
    }
    else {
        return rangeHighKey.compare(partition.lowKey) > 0;
    }
}

static bool intersectsRangeLowPartitionHigh(const Range& range, const PartitionRange& partition)
{
    if(partition.highKey.empty()) {
        return true;
    }

    return partition.highKey.compare(range.getLowKey()) > 0;
}

static bool intersect(const Range& range, const PartitionRange& partition)
{
    return intersectsRangeHighPartitionLow(range, partition) && intersectsRangeLowPartitionHigh(range, partition);
}

//
//  Map of partitions
//
class PartitionMap : public k2::PartitionMap
{
public:
    std::atomic_long ref_count {0};

    //
    // PartitionMap iterator. It is assumed that the value of the value of the underlying Range is not modified while iterating.
    //
    class iterator
    {
        friend class PartitionMap;
    private:
        // wrap the underlying set iterator
        std::set<PartitionDescription>::iterator _it;
        // null, it signifies iterator end
        const Range* _range;
    protected:
        iterator(std::set<PartitionDescription>::iterator iter)
        : _it(iter)
        , _range(nullptr)
        {
            // empty
        }

        iterator(std::set<PartitionDescription>::iterator iter, const Range& range)
        : _it(iter)
        , _range(&range)
        {
            // empty
        }

    public:
        iterator& operator++()
        {
            // we reached the end; a properly terminated partition mpa has to have an empty high key
            if(_it->getHighKey().empty()) {
                _range = nullptr;
            }

            // not checking for iterator end, it will be the same behavior as the underlying set iterator
            ++_it;

            if(_range == nullptr) {
                return *this;
            }

            // if the range and the partition do not intersect, we have reached the end
            if(!intersect(*_range, _it->range)) {
                _range = nullptr;
            }

            return *this;
        }

        bool operator== (const iterator& arg) const
        {
            return (arg._range == nullptr && _range == nullptr) || (arg._it == _it);
        }

        bool operator!= (const iterator& arg) const
        {
            return !(*this == arg);
        }

        PartitionDescription& operator* ()
        {
            return const_cast<PartitionDescription&>(*_it);
        }
    };

    iterator find(const Range& range)
    {
        // nothing to iterate over
        if(map.empty()) {
            return iterator(map.end());
        }

        auto it = map.end();

        if(range.getLowKey().empty()) {
            // rewind at the begining; this will take care of the less or equal case
            it = map.begin();

            return intersect(range, it->range) ? iterator(it, range) : iterator(map.end());
        }

        PartitionDescription desc;
        desc.range.lowKey = range.getLowKey();
        it = map.lower_bound(desc);

        if(it != map.begin()) {
            // case where the range is: [e] and partition: [d, f)
            --it;

            // case when range is: [d] and partition: [a,d)[d,f)
            if(!intersect(range, it->range)) {
                ++it;
            }
        }

        if(it == map.end()) {
            return iterator(it);
        }

        assert(intersect(range, it->range));

        return iterator(it, range);
    }

    iterator end()
    {
        return iterator(map.end());
    }
};

};  //  namespace client

};   //  namespace k2
