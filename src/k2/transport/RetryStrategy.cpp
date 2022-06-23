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

// Set the desired number of retries
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withRetries(int retries) {
    K2LOG_D(log::tx, "retries: {}", retries);
    _retries = retries;
    return *this;
}

// Set the exponential increase rate
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withRate(double rate) {
    K2LOG_D(log::tx, "rate: {}", rate);
    _rate = rate;
    return *this;
}

// Set the desired starting value
ExponentialBackoffStrategy& ExponentialBackoffStrategy::withStartTimeout(Duration startTimeout) {
    K2LOG_D(log::tx, "startTimeout: {}ms", k2::msec(startTimeout).count());
    _currentTimeout = startTimeout;
    return *this;
}

}  // namespace k2
