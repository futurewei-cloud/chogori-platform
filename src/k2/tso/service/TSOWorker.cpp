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

#include <algorithm>    // std::min
#include "seastar/core/sleep.hh"

#include <k2/common/Log.h>
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include "TSOService.h"
#include "Log.h"

namespace k2
{

seastar::future<> TSOService::TSOWorker::start()
{
    _tsoId = _outer.TSOId();

    RegisterGetTSOTimestampBatch();
    return seastar::make_ready_future<>();
}

seastar::future<> TSOService::TSOWorker::gracefulStop()
{
    // unregistar all APIs
    RPC().registerMessageObserver(dto::Verbs::GET_TSO_TIMESTAMP_BATCH, nullptr);
    return seastar::make_ready_future<>();
}

void TSOService::TSOWorker::RegisterGetTSOTimestampBatch()
{
    k2::RPC().registerMessageObserver(dto::Verbs::GET_TSO_TIMESTAMP_BATCH, [this](k2::Request&& request) mutable
    {
        if (request.payload)
        {
            uint16_t batchSize;
            request.payload->read((void*)&batchSize, sizeof(batchSize));

            // TODO: handle exceptions
            auto response = request.endpoint.newPayload();
            auto timestampBatch = GetTimestampFromTSO(batchSize);
            response->write(timestampBatch);
            return k2::RPC().sendReply(std::move(response), request);
        }
        else
        {
            K2LOG_E(log::tsoserver, "GetTSOTimestampBatch comes in without request payload.");
        }
        return seastar::make_ready_future();
    });
}

void TSOService::TSOWorker::UpdateWorkerControlInfo(const TSOWorkerControlInfo& controlInfo)
{
    if (_curControlInfo.IsReadyToIssueTS && controlInfo.IsReadyToIssueTS)
    {
        AdjustWorker(controlInfo);
    }
    else if (!_curControlInfo.IsReadyToIssueTS && controlInfo.IsReadyToIssueTS)
    {
        K2LOG_I(log::tsoserver, "StartWorker");

        // step 1/3 TODO: validate other member in controlInfo

        // step 2/3 Initialize statistics and kick off periodical statistics report task

        // step 3/3 set controlInfo and start/stop accepting request
        _curControlInfo = controlInfo;
    }
    else if (_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS)
    {
        K2LOG_I(log::tsoserver, "StopWorker");

        // step 1/3 TODO: validate other member in controlInfo

        // step 2/3 stop periodical statistics report task and report last residue statistics

        // step 3/3 set controlInfo and start/stop accepting request
        _curControlInfo = controlInfo;
    }
    else
    {
        // why we are doing this noop, is this a bug? let it crash in debug mode
        K2ASSERT(log::tsoserver, !_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS, "Noop update!");
        K2ASSERT(log::tsoserver, false, "!_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS");
    }
}

void TSOService::TSOWorker::AdjustWorker(const TSOWorkerControlInfo& controlInfo)
{
    K2LOG_D(log::tsoserver, "AdjustWorker: worker core" );

    // step 1/3 Validate current status and input
    K2ASSERT(log::tsoserver, controlInfo.IsReadyToIssueTS && _curControlInfo.IsReadyToIssueTS, "pre and post state need to be both ready!");
    // TODO: validate other member in controlInfo

    // step 2/3 process changed controlInfo, currently only need to pause worker when needed
    uint64_t timeToPauseWorkerNanoSec = 0;

    // when shrink uncertainty window by reduce ending time, worker need to wait out the delta
    if (controlInfo.TBEAdjustment < _curControlInfo.TBEAdjustment)
    {
        timeToPauseWorkerNanoSec += _curControlInfo.TBEAdjustment - controlInfo.TBEAdjustment;
    }

    // when reducing BatchTTL, worker need to wait out the delta (this should be rare)
    if (controlInfo.BatchTTL < _curControlInfo.BatchTTL)
    {
        timeToPauseWorkerNanoSec += _curControlInfo.BatchTTL - controlInfo.BatchTTL;
    }

    // when TBENanoSecStep change(this should be really really rare if not an bug), sleep 1 microsecond if no other reason to sleep
    if (controlInfo.TBENanoSecStep != _curControlInfo.TBENanoSecStep &&
        timeToPauseWorkerNanoSec < 1000)
    {
        timeToPauseWorkerNanoSec = 1000;
    }

    // round up to microsecond sleep time
    auto floorTimeToPauseWorkerNanoSec = timeToPauseWorkerNanoSec / 1000 * 1000;
    if (timeToPauseWorkerNanoSec > 0 && timeToPauseWorkerNanoSec != floorTimeToPauseWorkerNanoSec)
    {
        timeToPauseWorkerNanoSec = floorTimeToPauseWorkerNanoSec + 1000;
    }

    // wait out the required pause time if duration since last request is issued is smaller
    if (timeToPauseWorkerNanoSec > 0)
    {
        K2LOG_I(log::tsoserver, "AdjustWorker: worker core need to sleep(ns) {}", std::to_string(timeToPauseWorkerNanoSec));
        // Get current TBE(Timestamp Batch End) time and compare with last request time to see how much more need to pause
        uint64_t curTBEMicroSecRounded =  (now_nsec_count() +  _curControlInfo.TBEAdjustment) / 1000 * 1000;

        if ((curTBEMicroSecRounded - timeToPauseWorkerNanoSec) < _lastRequestTBEMicroSecRounded)
        {
            // TODO: warning if sleep more than 10 microsecond
            auto sleepNanoSecCount = _lastRequestTBEMicroSecRounded + timeToPauseWorkerNanoSec - curTBEMicroSecRounded;
            K2LOG_I(log::tsoserver, "Due to TSOWorkerControlInfo change, going to sleep {}ns", sleepNanoSecCount);
            if (sleepNanoSecCount >  10 * 1000)
            {
                K2LOG_W(log::tsoserver, "TSOWorkerControlInfo change trigger long sleep. Going to sleep {}ns", sleepNanoSecCount);
            }

            // busy sleep
            while ((curTBEMicroSecRounded - timeToPauseWorkerNanoSec) < _lastRequestTBEMicroSecRounded)
            {
                curTBEMicroSecRounded = (now_nsec_count() +  _curControlInfo.TBEAdjustment) / 1000 * 1000;
            }
        }
    }

    // step 3/3 set controlInfo and resume
    _curControlInfo =  controlInfo;
    return;
}

// API issuing Timestamp to the TSO client
TimestampBatch TSOService::TSOWorker::GetTimestampFromTSO(uint16_t batchSizeRequested)
{
    TimestampBatch result;

    K2LOG_D(log::tsoserver, "Start getting a timestamp batch");

    // this function is on hotpath, code organized to optimized the most common happy case for efficiency
    // In most of time, it is happy path, where current TBE(Timestamp Batch End) time at microsecond level(curTBEMicroSecRounded) is greater than last call's Timebatch end time
    // i.e. each worker core has one call or less per microsecond
    // In such case, simply issue timebatch associated with curTBEMicroSecRounded, timestamp counts up to either batchSizeRequested or max allowed from 1 microsec
    uint64_t curTBEMicroSecRounded = (now_nsec_count() +  _curControlInfo.TBEAdjustment) / 1000 * 1000;
    K2LOG_D(log::tsoserver, "Start getting a timestamp batch, got current time.");

    // most straightward happy case, fast path
    if (_curControlInfo.IsReadyToIssueTS &&
//        curTBEMicroSecRounded + 1000 < _curControlInfo.ReservedTimeShreshold &&
        curTBEMicroSecRounded > _lastRequestTBEMicroSecRounded)
    {
        if (curTBEMicroSecRounded + 1000 > _curControlInfo.ReservedTimeShreshold)
        {
            // this is really a bug if ReservedTimeShreshold is not updated promptly. 
            K2LOG_W(log::tsoserver, "Not ready to issue timestamp batch due to ReservedTimeShreshold exceeded curTime + 1000 and shreshold(us counts): {}:{}. BUGBUG- currently ignored",  curTBEMicroSecRounded + 1000, _curControlInfo.ReservedTimeShreshold);

            // BUGBUG 
            // Temporary ignore this error, remove this if block and commented out line 
            // in out if condition statement "curTBEMicroSecRounded + 1000 < _curControlInfo.ReservedTimeShreshold &&"
        }

        uint16_t batchSizeToIssue = std::min(batchSizeRequested, (uint16_t)(1000/_curControlInfo.TBENanoSecStep));

        result.TBEBase = curTBEMicroSecRounded + seastar::engine().cpu_id() - 1;
        result.TSOId = _tsoId;
        result.TsDelta = _curControlInfo.TsDelta;
        result.TTLNanoSec = _curControlInfo.BatchTTL;
        result.TSCount = batchSizeToIssue;
        result.TBENanoSecStep = _curControlInfo.TBENanoSecStep;

        /*K2LOG_I(log::tsoserver, "returning a tsBatch reqSize:" << batchSizeRequested << " at rounded request time:[" << curTBEMicroSecRounded
            << ":"<< _lastRequestTBEMicroSecRounded << "]TbeAdj:" << _curControlInfo.TBEAdjustment
            << " batch value(tbe:tsdelta:TTL:TSCount:Step)[" << result.TBEBase << ":" <<result.TsDelta << ":" << result.TTLNanoSec
            << ":" << batchSizeToIssue << ":" << result.TBENanoSecStep <<"]");
        */

        _lastRequestTBEMicroSecRounded = curTBEMicroSecRounded;
        _lastRequestTimeStampCount = batchSizeToIssue;

        // TODO: accumulate statistics

        return result;
    }

    // otherwise, handle less frequent situation
    return GetTimeStampFromTSOLessFrequentHelper(batchSizeRequested, curTBEMicroSecRounded);
}

// helper function to issue timestamp (or check error situation)
TimestampBatch TSOService::TSOWorker::GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t curTBEMicroSecRounded)
{
    K2LOG_I(log::tsoserver, "getting a timestamp batch in helper");
    // step 1/4 sanity check, check IsReadyToIssueTS and possible issued timestamp is within ReservedTimeShreshold
    if (!_curControlInfo.IsReadyToIssueTS)
    {
        K2LOG_W(log::tsoserver, "Not ready to issue timestamp batch due to IsReadyToIssueTS");

        // TODO: consider giving more detail information on why IsReadyToIssueTS is false, e.g. the instance is not master, not init, or wait/sleep
        throw TSONotReadyException();
    }

    // step 2/4 this is case when we try to issue timestamp batch beyond ReservedTimeShreshold (indicating it is not refreshed), this is really a bug and need to root cause.
    if (curTBEMicroSecRounded + 1000 > _curControlInfo.ReservedTimeShreshold)
    {
        // this is really a bug if ReservedTimeShreshold is not updated promptly.
         K2LOG_W(log::tsoserver, "Not ready to issue timestamp batch due to ReservedTimeShreshold exceeded curTime + 1000 and shreshold(us counts): {}:{}. BUGBUG- currently ignored",  curTBEMicroSecRounded + 1000, _curControlInfo.ReservedTimeShreshold);

        // BUGBUG need to throw here, for now lets just let go through 
        // Currently this happens as sometime the controller heartbeat/timesyn timed task was not promptly executed due to likely controller core being busy, thus TSOWorkerControlInfo was not properly updated on the worker. 
        // throw TSONotReadyException();
    }

    // step 3/4 if somehow current time is smaller than last request time
    if (curTBEMicroSecRounded < _lastRequestTBEMicroSecRounded)
    {
        // this is rare, normally should be a bug, add detal debug info later
        K2LOG_D(log::tsoserver, "curTBEMicroSecRounded: {} <_lastRequestTBEMicroSecRounded: {}", curTBEMicroSecRounded,_lastRequestTBEMicroSecRounded);
        // let client retry, maybe we should blocking sleep if there is a case this happening legit and the difference is small, e.g. a few microseconds
        throw TSONotReadyException();
    }

    // step 4/4 handle the case curTBEMicroSecRounded == _lastRequestTBEMicroSecRounded
    // If leftover timestamp of this microSec is sufficient, issue them out, otherwise, busy wait to next microsec and issue timestamp.
    K2ASSERT(log::tsoserver, curTBEMicroSecRounded == _lastRequestTBEMicroSecRounded, "last and this requests are in same microsecond!");
    uint16_t leftoverTS = 1000 / _curControlInfo.TBENanoSecStep - _lastRequestTimeStampCount;

    if (leftoverTS < batchSizeRequested)
    {
        // not enough timestamp at current microsecond to issue out,
        // busy wait out and go through normal code path to issue timestamp on next microsecond
        while (curTBEMicroSecRounded == _lastRequestTBEMicroSecRounded)
        {
            curTBEMicroSecRounded = (now_nsec_count() +  _curControlInfo.TBEAdjustment) / 1000 * 1000;
        }

        return GetTimestampFromTSO(batchSizeRequested);
    }

    K2ASSERT(log::tsoserver, curTBEMicroSecRounded == _lastRequestTBEMicroSecRounded, "last and this requests are in same microsecond!");

    TimestampBatch result;
    result.TBEBase = curTBEMicroSecRounded + seastar::engine().cpu_id() - 1 + _lastRequestTimeStampCount * _curControlInfo.TBENanoSecStep;
    result.TSOId = _tsoId;
    result.TsDelta = _curControlInfo.TsDelta;
    result.TTLNanoSec = _curControlInfo.BatchTTL;
    result.TSCount = batchSizeRequested;
    result.TBENanoSecStep = _curControlInfo.TBENanoSecStep;

    //_lastRequestTBEMicroSecRounded = curTBEMicroSecRounded;   // they are same, no need to set.
    _lastRequestTimeStampCount += batchSizeRequested; // we've just issued batchSizeRequested

    return result;
}

}
