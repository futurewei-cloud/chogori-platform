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
#include <seastar/core/timer.hh>
#include <seastar/core/future.hh>

namespace k2 {

// the timer allows users to run tasks which are future-based
// it also guarantees that there is only one execution going on at a time
class SingleTimer {
public:
    SingleTimer() {}

    template <typename Func>
    SingleTimer(Func&& func) {
        setCallback(std::move(func));
    }

    template <typename Func>
    void setCallback(Func&& func) {
        _timer.set_callback([this, func=std::move(func)] {
            _timerDone = _timerDone.then([this, func=std::move(func)] {
                return func();
            });
        });
    }

    seastar::future<> stop() {
        _timer.cancel();
        return std::move(_timerDone);
    };

    void arm(Duration when) {
        _timer.arm(when);
    }

    bool isArmed() const {
        return _timer.armed();
    }

private:
    seastar::timer<> _timer;
    seastar::future<> _timerDone = seastar::make_ready_future();

};  // class Timer

// This periodic timer, similarly to the SingleTimer allows for safe running of future-based events.
// It also (as opposed to ss:timer) schedules the next run to be executed AFTER the current run is done executing.
// For example if we schedule with 10ms recurrence, this timer will run 10ms AFTER the current run is done executing.
// That means that we'll have guaranteed pause of 10ms between executions.
// in contrast, with ss::timer, periodic events run on each 10ms wall clock tick, no matter how long the event takes.
// This is not desirable in cases where we want to ensure there is a 10ms delay between executions.
class PeriodicTimer {
public:
    PeriodicTimer() {}

    template <typename Func>
    PeriodicTimer(Func&& func) {
        setCallback(std::move(func));
    }

    template <typename Func>
    void setCallback(Func&& func) {
        _timer.set_callback([this, func = std::move(func)] {
            if (!_isStopped) {
                _timerDone = _timerDone.then([this, func = std::move(func)] {
                    return func();
                })
                .then([this] {
                    if (!_isStopped) {
                        _timer.arm(_when);
                    }
                });
            }
        });
    }

    void cancel() {
        _timer.cancel();
        _isStopped = true;
    }

    seastar::future<> stop() {
        cancel();
        return std::move(_timerDone);
    };

    void armPeriodic(Duration when) {
        _when=when;
        _isStopped=false;
        _timer.arm(when);
    }

    bool isArmed() const {
        return !_isStopped;
    }

private:
    seastar::timer<> _timer;
    bool _isStopped=true;
    Duration _when{};
    seastar::future<> _timerDone = seastar::make_ready_future();

};  // class PeriodicTimer

} // ns k2
