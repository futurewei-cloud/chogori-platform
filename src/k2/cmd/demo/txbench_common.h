//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>

#pragma once

// The message verbs supported by this service
enum MsgVerbs: k2::Verb {
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
    BenchSession(const k2::TXEndpoint& client, uint64_t sessionID, const SessionConfig& config):
        client(client),
        sessionID(sessionID),
        config(config),
        totalSize(0),
        totalCount(0),
        unackedSize(0),
        unackedCount(0),
        runningSum(0) {
    }

    k2::TXEndpoint client;
    uint64_t sessionID;
    SessionConfig config;
    uint64_t totalSize;
    uint64_t totalCount;
    uint64_t unackedSize;
    uint64_t unackedCount;
    uint64_t runningSum;
};
