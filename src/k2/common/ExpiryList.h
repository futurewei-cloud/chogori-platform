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

namespace k2 {
namespace nsbi = boost::intrusive;

template <class ElemT, class ClockT=Clock>
class ExpiryList {
public:
    typedef std::function<seastar::future<>(ElemT&)> Func;

    void start(Duration timeout, Func&& cleanupFn) {
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
        _expiryTimer.armPeriodic(timeout/2);
     }

    seastar::future<> stop() {
        return _expiryTimer.stop()
            .then([this] {
                _list.clear();
             });
    }

    // Move elment to the end of the TS ordered list
    void moveLast(ElemT& elem) {
        // new TS needs to be >= current maximum TS
        if (elem.expiry() < _list.back().expiry()) {
            assert(false);
            throw std::invalid_argument("too small ts");
        }
        _list.erase(_list.iterator_to(elem));
        _list.push_back(elem);
    }

    void erase(ElemT& elem) {
        _list.erase(_list.iterator_to(elem));
    }

    void add(ElemT& elem) {
        _list.push_back(elem);
    }


private:
    typedef nsbi::list<ElemT, nsbi::member_hook<ElemT, nsbi::list_member_hook<>, &ElemT::tsLink>> IntrusiveList;
    IntrusiveList _list;
    // heartbeats checks are driven off single timer.
    PeriodicTimer _expiryTimer;
};
}
