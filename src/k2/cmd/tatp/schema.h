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

#include <decimal/decimal>
#include <string>

#include <k2/common/Common.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/transport/Payload.h>
#include <k2/transport/PayloadSerialization.h>

#include "tatp_rand.h"
#include "Log.h"
using namespace k2;

#define CHECK_READ_STATUS(read_result) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tpcc, "TATP failed to read rows: {}", (read_result).status); \
            return make_exception_future(std::runtime_error(String("TATP failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

#define CHECK_READ_STATUS_TYPE(read_result,type) \
    do { \
        if (!((read_result).status.is2xxOK())) { \
            K2LOG_D(log::tpcc, "TATP failed to read rows: {}", (read_result).status); \
            return make_exception_future<type>(std::runtime_error(String("TATP failed to read rows: ") + __FILE__ + ":" + std::to_string(__LINE__))); \
        } \
    } \
    while (0) \

class Subscriber {
public:
    static inline dto::Schema subscriber_schema {
        .name = "subscriber",
        .version = 1, 
        .fields = std::vector<dto::SchemaField> {
            {dto::FieldType::INT16T, "id"},
        }
    };
};
