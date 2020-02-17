#pragma once
#include <chrono>
#include <exception>

#include "k2/common/Log.h"
#include "k2/common/TimeStamp.h"
#include <k2/transport/RPCTypes.h>

namespace k2
{
// APIs between TSO client and TSO Server
enum TSOMsgVerbs : k2::Verb {
    GET_TSO_MASTERSERVER_URL    = 100,  // API from TSO client to any TSO instance to get master instance URL
    GET_TSO_WORKERS_URLS        = 101,  // API from TSO client to TSO master server to get its workers(cores)' URLs 
    GET_TSO_TIMESTAMP_BATCH     = 102,  // API from TSO client to get timestamp batch from any TSO worker cores
    ACK_TSO                     = 103   // ACK to TSO client for above APIs
};

// type definitions common between TSO client and TSO server
class TimeStampBatch
{
public:

    uint64_t    TbeTESBase;     // batch uncertain window end time(system_clock), number of nanosecond ticks from UTC 1970-01-01:00:00:00
    uint32_t    TSOId;          // TSOId
    uint16_t    TsDelta;        //  time difference between Ts and Te, in nanosecond unit
    uint16_t    TTLNanoSec;     //  TTL of batch on the client side in nanoseconds
    uint8_t     TSCount;        //  number of timestamp can be generated from this batch
    uint8_t     TbeNanoSecStep; //  step (number of nanoseconds) to skip between timestamp Te in the batch


    // static helper to get  Timestamp from batch
    // caller is responsible to verify usedCount < TSCount and to increment usedCount after call
    static const TimeStamp GenerateTimeStampFromBatch(const TimeStampBatch& batch, uint8_t usedCount)
    {
        K2ASSERT(usedCount < batch.TSCount, "requested timestamp count too large.");

        uint16_t endingNanoSecAdjust = usedCount * batch.TbeNanoSecStep;
        // creat timestamp from batch. Note: tStart are the same for all timestamps in the batch
        TimeStamp ts(batch.TbeTESBase + endingNanoSecAdjust, batch.TsDelta + endingNanoSecAdjust, batch.TSOId);
        return ts;
    }

    DEFAULT_COPY_MOVE_INIT(TimeStampBatch);
    K2_PAYLOAD_FIELDS(TbeTESBase, TSOId, TsDelta, TTLNanoSec, TSCount, TbeNanoSecStep);

};

// TSO service should be started with at least two cores, one controller and rest are workers.
class NotEnoughCoreException : public std::exception {
public:
    NotEnoughCoreException(uint16_t coreCount) : _coreCount(coreCount) {};

private:
    virtual const char* what() const noexcept override { return "NotEnoughCoreException: core counts" + _coreCount; }

    uint16_t _coreCount {0};
};

// TSO server not ready yet to issue timestamp(batch)
// TODO: add more detail error info.
class ServerNotReadyException : public std::exception {
    private:
    virtual const char* what() const noexcept override { return "Server not ready to issue timestamp, please retry later."; }
};
}
