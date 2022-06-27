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
#include <boost/intrusive/list.hpp>
#include "Timer.h"
#include "Log.h"

namespace k2 {
namespace nsbi = boost::intrusive;

// Utility class to keep elements orderded by expiry time and cleanup elements upon expiry.
//
// Template members:
//
//  ElemT: Element type, it required to have a  member expiry() returning expiry time.
//  Field: boost::intrusive::list_member_hook<> type field.
//  ClockT: Clock used to get current time.
//
template <class ElemT, nsbi::list_member_hook<> ElemT::* Field, class ClockT=Clock>
class ExpiryList {
public:
    typedef std::function<seastar::future<>(ElemT&)> Func;

    // Start a periodic timer to cleanup elements upon expiry.
    // Args:
    // interval:  Periodic timer interval, Elemnets may take upto interval after
    //            expiry for clean up depending on when the timer is fired.
    // cleanupFn: Supplied function to clean up the element.
    void start(Duration interval, Func&& cleanupFn) {
        _expiryTimer.setCallback([this,  func=std::move(cleanupFn)] {
            return seastar::do_until(
                [this] {
                    return _list.empty() || _list.front().expiry() > ClockT::now();
                },
                [this, func=std::move(func)] {
                    auto& elem = _list.front();
                    _list.pop_front();
                    return func(elem);
                });

        });
        _expiryTimer.armPeriodic(interval);
     }

    // Stop the timer.
    seastar::future<> stop() {
        return _expiryTimer.stop()
            .then([this] {
                _list.clear();
             });
    }

    // Move elment to the end of the time ordered list
    void moveToEnd(ElemT& elem) {
        // The list is supposed to be ordered by time, as it only checks front
        // element for expiry. Adding smaller time at end
        // indicates logic error somewhere.
        K2ASSERT(log::common, elem.expiry() >= _list.back().expiry(), "too small ts");
        if (((elem.*Field).is_linked())) _list.erase(_list.iterator_to(elem));
        _list.push_back(elem);
    }

    void erase(ElemT& elem) {
        if (!((elem.*Field).is_linked())) return;
        _list.erase(_list.iterator_to(elem));
    }

    void add(ElemT& elem) {
        _list.push_back(elem);
    }


private:
    typedef nsbi::list<ElemT, nsbi::member_hook<ElemT, nsbi::list_member_hook<>, Field>> IntrusiveList;
    IntrusiveList _list;
    // Expiry checks are driven off single timer.
    PeriodicTimer _expiryTimer;
};
}
