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

#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>

#pragma once
using namespace k2;

// The message verbs supported by this service
enum MsgVerbs: Verb {
    REQUEST_COPY = 10, // issue request with a copy
    REQUEST = 11, // incoming requests
    START_SESSION = 12 // used to start a new test session
};

struct BenchSession {
    BenchSession()=default;
    BenchSession(BenchSession&&)=default;
    BenchSession& operator=(BenchSession&&)=default;
    BenchSession(uint64_t sessionId, size_t dataSize): sessionId(sessionId) {
        dataCopy = String(dataSize, '.');
        dataShare.write(dataCopy);
    }

    uint64_t sessionId = 0;
    uint64_t totalSize=0;
    uint64_t totalCount=0;
    Payload dataShare{Payload::DefaultAllocator};
    String dataCopy;
    std::vector<std::unique_ptr<TXEndpoint>> endpoints;
};

struct TXBenchStartSession {
    uint64_t responseSize = 0;
    K2_PAYLOAD_FIELDS(responseSize);
};

struct TXBenchStartSessionAck {
    uint64_t sessionId=0;
    K2_PAYLOAD_FIELDS(sessionId);
};

template <typename ValueType>
struct TXBenchRequest {
    uint64_t sessionId=0;
    SerializeAsPayload<ValueType> data;
    K2_PAYLOAD_FIELDS(sessionId, data);
};

template <typename ValueType>
struct TXBenchResponse {
    uint64_t sessionId=0;
    SerializeAsPayload<ValueType> data;
    K2_PAYLOAD_FIELDS(sessionId, data);
};
