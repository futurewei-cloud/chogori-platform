//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "RetryStrategy.h"

namespace k2 {

ExponentialBackoffStrategy::ExponentialBackoffStrategy() : _retries(3),
                                                           _try(0),
                                                           _rate(5),
                                                           _currentTimeout(1us),
                                                           _success(false),
                                                           _used(false) {
    K2DEBUG("ctor retries " << _retries << ", rate " << _rate << ", startTimeout "
                            << k2::usec(_currentTimeout).count() << "ms");
}

// destructor
ExponentialBackoffStrategy::~ExponentialBackoffStrategy() {
    K2DEBUG("dtor");
}

// Set the desired number of retries
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withRetries(int retries) {
    K2DEBUG("retries: " << retries);
    _retries = retries;
    return *this;
}

// Set the exponential increase rate
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withRate(int rate) {
    K2DEBUG("rate: " << rate);
    _rate = rate;
    return *this;
}

// Set the desired starting value
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withStartTimeout(Duration startTimeout) {
    K2DEBUG("startTimeout: " << k2::msec(startTimeout).count() << "ms");
    _currentTimeout = startTimeout;
    return *this;
}

}  // namespace k2
