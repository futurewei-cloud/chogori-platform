//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

#pragma once

// third-party
#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>

// k2
#include "common/Common.h"
#include "common/Log.h"
#include "RPCDispatcher.h"

namespace k2 {
// This file defines a few retry strategies that can be used in communication

// an Exponential backoff strategy, with parameters retries, rate, and startTimeout.
// when Do() is invoked with some function, the function is repeatedly called with
// the remaining retries and the timeoutValue it should use.
// the timeoutvalue is calculated based on exponential increase:
// timeoutValue = startTimeout * ((rate)**retryIndex)
class ExponentialBackoffStrategy {
public: // types
    // this is returned in an exceptional future if you attempt to call Do() more than once
    class DuplicateExecutionException : public std::exception {};

public: // lifecycle
    // create a new ExponentialBackoffStrategy
    ExponentialBackoffStrategy():
        _retries(3),
        _try(0),
        _rate(5),
        _currentTimeout(std::chrono::microseconds(1)),
        _success(false),
        _used(false) {
        K2DEBUG("ctor retries " << _retries <<", rate " << _rate << ", startTimeout " << _currentTimeout.count());
    }

    // destructor
    ~ExponentialBackoffStrategy(){
        K2DEBUG("dtor");
    }

    // Set the desired number of retries
    ExponentialBackoffStrategy& WithRetries(int retries){
        K2DEBUG("retries: " << retries);
        _retries = retries;
        return *this;
    }

    // Set the exponential increase rate
    ExponentialBackoffStrategy& WithRate(int rate) {
        K2DEBUG("rate: " << rate);
        _rate = rate;
        return *this;
    }

    // Set the desired starting value
    ExponentialBackoffStrategy& WithStartTimeout(Duration startTimeout) {
        K2DEBUG("startTimeout: " << startTimeout.count());
        _currentTimeout = startTimeout;
        return *this;
    }

public: // API
    // Execute the given function until it either succeeds or we exhaust the retries. If the retries are
    // exhausted, then we return the exception tossed from the last run.
    // Note that we do not setup any timeout timers here. We just provide the correct value to use
    template<typename Func>
    seastar::future<> Do(Func&& func) {
        K2DEBUG("Initial Do");
        if (_used) {
            K2WARN("This strategy has already been used");
            return seastar::make_exception_future<>(DuplicateExecutionException());
        }
        auto resultPtr = seastar::make_lw_shared<>(seastar::make_ready_future<>());
        return seastar::do_until(
            [this] { return _success || this->_try >= this->_retries; },
            [this, func=std::move(func), resultPtr] {
                this->_try++;
                this->_currentTimeout*=this->_try;
                K2DEBUG("running try " << this->_try << ", with timeout " << this->_currentTimeout.count());
                return func(this->_retries - this->_try, this->_currentTimeout).
                    handle_exception_type([](RPCDispatcher::DispatcherShutdown&) {
                        K2DEBUG("Dispatcher has shut down. Stopping retry");
                        return seastar::make_ready_future<>();
                    }).
                    then_wrapped([this, resultPtr](auto&& fut) {
                        // the func future is done.
                        // if we exited with success, then we shouldn't run anymore
                        _success = !fut.failed();
                        resultPtr->ignore_ready_future(); // ignore previous result stored in the result
                        (*resultPtr.get()) = std::move(fut);
                        K2DEBUG("round ended with success=" << _success);
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
    int _retries;
    // which try we're on
    int _try;
    // the exponential growth rate
    int _rate;
    // the value of the current timeout
    Duration _currentTimeout;
    // indicate if the latest round has succeeded (so that we can break the retry loop)
    bool _success;
    // indicate if this strategy has been used already so that we can reject duplicate attempts to use it
    bool _used;

private: // don't need
    ExponentialBackoffStrategy(const ExponentialBackoffStrategy& o) = delete;
    ExponentialBackoffStrategy(ExponentialBackoffStrategy&& o) = delete;
    ExponentialBackoffStrategy& operator=(const ExponentialBackoffStrategy& o) = delete;
    ExponentialBackoffStrategy& operator=(ExponentialBackoffStrategy&& o) = delete;

}; // ExponentialBackoffStrategy

} // k2
