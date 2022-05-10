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
#include "Timer.h"

namespace k2 {
namespace nsbi = boost::intrusive;

// Utility map class that also keeps a timestamp ordered elements list
template <class KeyT, class ValT> class TimestampOrderedMap {
public:
    class MapElem {
    public:
        ValT& getValue() {return entry;}
        TimePoint timestamp() {return ts;}
        KeyT key;
        ValT entry;
        TimePoint ts;
        nsbi::list_member_hook<> tsLink;
    };

    typedef typename std::unordered_map<KeyT, MapElem>::iterator iterator;
    typedef nsbi::list<MapElem, nsbi::member_hook<MapElem, nsbi::list_member_hook<>, &MapElem::tsLink>> TsOrderedList;


    auto insert(const KeyT &key, ValT&& val, TimePoint ts) {
        auto pair = elems.emplace(key, MapElem  {
            .key = key,
            .entry = std::move(val),
            .ts = ts,
            .tsLink = {},
        });
        MapElem& elem = pair.first->second;
        tsOrderedList.push_back(elem);
    }

    // TODO: Implement using boost iterator helpers to reduce boilerplate code.
    iterator end() {return elems.end();}
    iterator begin() { return elems.begin();}
    iterator find(const KeyT &key) {return  elems.find(key);}
    ValT& at(const KeyT &key) {return elems.at(key).getValue();}
    auto size() {return elems.size();}
    void unlink(iterator iter) {
        tsOrderedList.erase(tsOrderedList.iterator_to(iter->second));
    }
    auto extract(iterator iter) {
        unlink(iter);
        // Remove from map
        return elems.extract(iter);
    }
    auto erase(const KeyT &key) {
        iterator iter = elems.find(key);
        unlink(iter);
        return elems.erase(iter);
    }

    void resetTs(iterator iter, TimePoint ts) {
        // new TS needs to be >= current maximum TS
        if (ts < tsOrderedList.back().ts) {
            assert(false);
            throw std::invalid_argument("too small ts");
        }
        unlink(iter);
        iter->second.ts = ts;
        tsOrderedList.push_back(iter->second);
    }

    iterator getFirst() {
        if (tsOrderedList.empty())
            return elems.end();
        return find(tsOrderedList.front().key);
    }

    TsOrderedList tsOrderedList;
    std::unordered_map<KeyT, MapElem> elems;
};

template <class KeyT, class ValT, class ClockT=Clock>
class MapWithExpiry {
public:
    template <typename Func>
    void start(Duration timeout, Func&& func) {
        _timeout = timeout;
        _expiryTimer.setCallback([this, func = std::move(func)] {
            return seastar::do_until(
                [this] {
                    auto iter = _elems.getFirst();
                    return (iter == _elems.end() || iter->second.timestamp() > ClockT::now());
                },
                [this, func] {
                    auto iter = _elems.getFirst();
                    // First Remove element from map before calling callback
                    auto node = _elems.extract(iter);
                    if (!node) return seastar::make_ready_future();
                    return func(node.key(), node.mapped().getValue());
                });

        });
        _expiryTimer.armPeriodic(_timeout/2);
     }

    seastar::future<> stop() {
        return _expiryTimer.stop()
            .then([this] {
                std::vector<seastar::future<>> bgFuts;
                for(auto iter = _elems.begin(); iter != _elems.end(); iter++) {
                    _elems.unlink(iter);
                    bgFuts.push_back(iter->second.getValue().cleanup().discard_result());
                }
                return seastar::when_all_succeed(bgFuts.begin(), bgFuts.end());
            });
    }

    typedef typename TimestampOrderedMap<KeyT, ValT>::iterator iterator;

    // Move elment to the end of the TS ordered list
    void reorder(iterator iter) {_elems.resetTs(iter, getExpiry());}
    iterator find(const KeyT &key) {return _elems.find(key);}
    TimePoint getExpiry() {return ClockT::now() + _timeout;}
    void insert(const KeyT &key, ValT&& val) {_elems.insert(key, std::move(val), getExpiry());}
    void erase(const KeyT &key) {_elems.erase(key);}
    auto size() {return _elems.size(); }
    ValT& at(const KeyT &key) {return _elems.at(key);}
    iterator end() {return _elems.end();}
    ValT& getValue(iterator it) {return it->second.getValue();}

private:
    TimestampOrderedMap<KeyT, ValT> _elems;
    // heartbeats checks are driven off single timer.
    PeriodicTimer _expiryTimer;
    Duration _timeout = 10s;

};
}
