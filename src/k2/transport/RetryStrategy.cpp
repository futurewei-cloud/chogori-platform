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
