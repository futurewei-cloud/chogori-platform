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

#include <seastar/core/future.hh>
#include <k2/config/Config.h>

#include <vector>

#include "schema.h"
#include "tatp_rand.h"

typedef std::vector<std::function<seastar::future<k2::WriteResult>(k2::K2TxnHandle&)>> TATPData;

const uint32_t SUBSCRIBER_COUNT = 100000;

struct TATPDataGen {
    seastar::future<TATPData> generateSubscriberData() {
        TATPData data;
        data.reserve(SUBSCRIBER_COUNT);
        return seastar::do_with(
            std::move(data), 
            RandContext(0),
            boost::irange(1, SUBSCRIBER_COUNT+1),
            [this] (auto& data, auto& random, auto& range) {

            }
        );
    }

    seastar::future<TATPData> generateAcessData() {

    }

    seastar::future<TATPData> generateSpecialFacilityData() {

    }

    seastar::future<TATPData> generateCallForwardingData() {
        
    }
};
