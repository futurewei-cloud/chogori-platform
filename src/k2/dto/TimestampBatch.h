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
#include <chrono>
#include <exception>

#include "k2/common/Log.h"
#include "Timestamp.h"
#include <k2/transport/RPCTypes.h>
#include "Log.h"
namespace k2
{
namespace dto
{
// timestampBatch between TSO client and TSO server
struct TimestampBatch
{
    uint64_t    TBEBase;        // Timestamp Batch uncertain window End time base, number of nanosecond ticks from TAI
    uint32_t    TSOId;          // TSOId
    uint16_t    TsDelta;        //  time difference between Ts and Te, in nanosecond unit
    uint16_t    TTLNanoSec;     //  TTL of batch on the client side in nanoseconds
    uint8_t     TSCount;        //  number of timestamp can be generated from this batch
    uint8_t     TBENanoSecStep; //  step (number of nanoseconds) to skip between timestamp Te in the batch


    // static helper to get  Timestamp from batch
    // caller is responsible to verify usedCount < TSCount and to increment usedCount after call
    static const Timestamp generateTimeStampFromBatch(const TimestampBatch& batch, uint8_t usedCount)
    {
        K2ASSERT(log::dto, usedCount < batch.TSCount, "requested timestamp count too large.");

        uint16_t endingNanoSecAdjust = usedCount * batch.TBENanoSecStep;
        // creat timestamp from batch. Note: tStart are the same for all timestamps in the batch
        Timestamp ts(batch.TBEBase + endingNanoSecAdjust, batch.TSOId, batch.TsDelta + endingNanoSecAdjust);
        return ts;
    }

    K2_PAYLOAD_FIELDS(TBEBase, TSOId, TsDelta, TTLNanoSec, TSCount, TBENanoSecStep);
    K2_DEF_FMT(TimestampBatch, TBEBase, TSOId, TsDelta, TTLNanoSec, TSCount, TBENanoSecStep);
};

struct GetTimeStampBatchRequest
{
    uint16_t batchSizeRequested = 8;

    K2_PAYLOAD_FIELDS(batchSizeRequested);
    K2_DEF_FMT(GetTimeStampBatchRequest, batchSizeRequested);
};

struct GetTimeStampBatchResponse
{
    TimestampBatch timeStampBatch;

    K2_PAYLOAD_FIELDS(timeStampBatch);
    K2_DEF_FMT(GetTimeStampBatchResponse, timeStampBatch);
};

struct GetTSOServerURLsRequest
{
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(GetTSOServerURLsRequest);
};

struct GetTSOServerURLsResponse
{
    std::vector<String> serverURLs;

    K2_PAYLOAD_FIELDS(serverURLs);
    K2_DEF_FMT(GetTSOServerURLsResponse, serverURLs);
};

struct GetTSOServiceNodeURLsRequest
{
    K2_PAYLOAD_EMPTY;
    K2_DEF_FMT(GetTSOServiceNodeURLsRequest);
};

struct GetTSOServiceNodeURLsResponse
{
    std::vector<std::vector<String>> serviceNodeURLs;

    K2_PAYLOAD_FIELDS(serviceNodeURLs);
    K2_DEF_FMT(GetTSOServiceNodeURLsResponse, serviceNodeURLs);
};

}  // dto
}  // k2
