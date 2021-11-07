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

// third-party
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

// k2
#include <k2/common/Common.h>
#include <k2/common/Log.h>
#include <k2/common/Chrono.h>
#include "RPCDispatcher.h"
#include "Log.h"

namespace k2 {
// this is returned in an exceptional future if you attempt to call Do() more than once
class DuplicateExecutionException : public std::exception {};

// this can be returned by retryable code to indicate that no more retries should be performed
class StopRetryException: public std::exception{};

// an Exponential backoff strategy, with parameters retries, rate, and startTimeout.
// When Do() is invoked with some function, the function is repeatedly called with the remaining retries and the
// timeoutValue it should use. The timeoutvalue is calculated based on exponential increase.
// The callback is invoked until it returns a successful future. If it returns an exceptional future, we keep retrying
class ExponentialBackoffStrategy {
public:
    // Set the desired number of retries
    ExponentialBackoffStrategy& withRetries(int retries);

    // Set the exponential increase rate
    ExponentialBackoffStrategy& withRate(int rate);

    // Set the desired starting value
    ExponentialBackoffStrategy& withStartTimeout(Duration startTimeout);

public: // API
    // Execute the given function until it either succeeds or we exhaust the retries. If the retries are
    // exhausted, then we return the exception tossed from the last run.
    // Note that we do not setup any timeout timers here. We just provide the correct value to use
    template<typename Func>
    seastar::future<> run(Func&& func) {
        K2LOG_D(log::tx, "Initial run");
        if (_used) {
            K2LOG_W(log::tx, "This strategy has already been used");
            return seastar::make_exception_future<>(DuplicateExecutionException());
        }
        auto resultPtr = seastar::make_lw_shared<>(
            seastar::make_exception_future<>(RPCDispatcher::RequestTimeoutException()));
        return seastar::do_until(
            [this] { return _success || this->_try >= this->_retries; },
            [this, func=std::move(func), resultPtr] ()mutable{
                this->_try++;
                this->_currentTimeout*=this->_try;
                K2LOG_D(log::tx, "running try {}, with timeout {}ms", this->_try, k2::msec(_currentTimeout).count());
                return func(this->_retries - this->_try, this->_currentTimeout).
                    handle_exception_type([this](RPCDispatcher::DispatcherShutdown&) {
                        K2LOG_D(log::tx, "Dispatcher has shut down. Stopping retry");
                        this->_try = this->_retries; // ff to the last retry
                        return seastar::make_exception_future<>(RPCDispatcher::RequestTimeoutException());
                    }).
                    handle_exception_type([this](StopRetryException&) {
                        K2LOG_D(log::tx, "Stopping retry due to explicit request to stop retrying");
                        this->_try = this->_retries; // ff to the last retry
                        return seastar::make_exception_future<>(RPCDispatcher::RequestTimeoutException());
                    }).
                    then_wrapped([this, resultPtr](auto&& fut) {
                        // the func future is done.
                        // if we exited with success, then we shouldn't run anymore
                        _success = !fut.failed();
                        resultPtr->ignore_ready_future(); // ignore previous result stored in the result
                        (*resultPtr.get()) = std::move(fut);
                        K2LOG_D(log::tx, "round ended with success={}", _success);
                        return seastar::make_ready_future<>();
                    });
        }).then_wrapped([this, resultPtr](auto&& fut){
            // this is the future returned by the do_until loop. we don't need it so just ignore it.
            fut.ignore_ready_future();
            return std::move(*resultPtr.get());
        });
    }

private: // fields
    // how many times we should retry
    int _retries{3};
    // which try we're on
    int _try{0};
    // the exponential growth rate
    int _rate{5};
    // the value of the current timeout
    Duration _currentTimeout{1us};
    // indicate if the latest round has succeeded (so that we can break the retry loop)
    bool _success{false};
    // indicate if this strategy has been used already so that we can reject duplicate attempts to use it
    bool _used{false};
}; // ExponentialBackoffStrategy

} // k2
