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
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>

using namespace k2;

// The message verbs supported by this service
enum MsgVerbs: Verb {
    GET_DATA_URL = 10, // used to discover the data URL for a node (e.g. rdma or tcp url)
    REQUEST = 11, // incoming requests
    ACK = 12, // ACKS for requests
    START_SESSION = 13 // used to start a new test session
};

struct SessionConfig {
    bool echoMode;
    uint64_t responseSize;
    uint64_t pipelineSize;
    uint64_t pipelineCount;
    uint64_t ackCount;
    K2_DEF_TO_STREAM_JSON_OPS_INTR(SessionConfig, echoMode, responseSize, pipelineSize, pipelineCount, ackCount);
};

struct Ack {
    uint64_t sessionID;
    uint64_t totalCount;
    uint64_t totalSize;
    uint64_t checksum;
};

struct SessionAck {
    uint64_t sessionID;
};

struct BenchSession {
    BenchSession():
        sessionID(0),
        config(SessionConfig{0,0,0,0,0}),
        totalSize(0),
        totalCount(0),
        unackedSize(0),
        unackedCount(0),
        runningSum(0) {}
    BenchSession(BenchSession&&) = default;
    ~BenchSession() = default;
    BenchSession(const TXEndpoint& client, uint64_t sessionID, const SessionConfig& config):
        client(client),
        sessionID(sessionID),
        config(config),
        totalSize(0),
        totalCount(0),
        unackedSize(0),
        unackedCount(0),
        runningSum(0) {
    }

    TXEndpoint client;
    uint64_t sessionID;
    SessionConfig config;
    uint64_t totalSize;
    uint64_t totalCount;
    uint64_t unackedSize;
    uint64_t unackedCount;
    uint64_t runningSum;
};
