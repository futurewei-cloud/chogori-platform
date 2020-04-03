#pragma once
#include <chrono>
#include <exception>

#include "k2/common/Log.h"
#include "TSO.h"
#include <k2/transport/RPCTypes.h>

namespace k2
{
namespace dto
{
// timestampBatch between TSO client and TSO server
class TimestampBatch
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
    static const Timestamp GenerateTimeStampFromBatch(const TimestampBatch& batch, uint8_t usedCount)
    {
        K2ASSERT(usedCount < batch.TSCount, "requested timestamp count too large.");

        uint16_t endingNanoSecAdjust = usedCount * batch.TbeNanoSecStep;
        // creat timestamp from batch. Note: tStart are the same for all timestamps in the batch
        Timestamp ts(batch.TbeTESBase + endingNanoSecAdjust, batch.TSOId, batch.TsDelta + endingNanoSecAdjust);
        return ts;
    }

    DEFAULT_COPY_MOVE_INIT(TimestampBatch);
    K2_PAYLOAD_FIELDS(TbeTESBase, TSOId, TsDelta, TTLNanoSec, TSCount, TbeNanoSecStep);

};
}  // dto
}  // k2 
