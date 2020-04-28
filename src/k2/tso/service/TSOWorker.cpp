#include <algorithm>    // std::min
#include "seastar/core/sleep.hh"

#include <k2/common/Log.h>
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include "TSOService.h"

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
            //K2INFO("time stamp batch returned is: " << timestampBatch);
            response->write(timestampBatch);
            k2::RPC().sendReply(std::move(response), request);
        }
        else
        {
            K2ERROR("GetTSOTimestampBatch comes in without request payload.");
        }

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
        K2INFO("StartWorker: worker core:" << seastar::engine().cpu_id());

        // step 1/3 TODO: validate other member in controlInfo

        // step 2/3 Initialize statistics and kick off periodical statistics report task

        // step 3/3 set controlInfo and start/stop accepting request
        _curControlInfo = controlInfo;
    }
    else if (_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS)
    {
        K2INFO("StopWorker: worker core:" << seastar::engine().cpu_id());

        // step 1/3 TODO: validate other member in controlInfo

        // step 2/3 stop periodical statistics report task and report last residue statistics

        // step 3/3 set controlInfo and start/stop accepting request
        _curControlInfo = controlInfo;
    }
    else
    {
        // why we are doing this noop, is this a bug? let it crash in debug mode
        K2ASSERT(!_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS, "Noop update!");
        K2ASSERT(false, "!_curControlInfo.IsReadyToIssueTS && !controlInfo.IsReadyToIssueTS");
    }
}

void TSOService::TSOWorker::AdjustWorker(const TSOWorkerControlInfo& controlInfo)
{
    //K2INFO("AdjustWorker: worker core" );

    // step 1/3 Validate current status and input
    K2ASSERT(controlInfo.IsReadyToIssueTS && _curControlInfo.IsReadyToIssueTS, "pre and post state need to be both ready!");
    // TODO: validate other member in controlInfo

    // step 2/3 process changed controlInfo, currently only need to pause worker when needed
    uint64_t timeToPauseWorkerNanoSec = 0;

    // when shrink uncertainty window by reduce ending time, worker need to wait out the delta
    if (controlInfo.TbeTESAdjustment < _curControlInfo.TbeTESAdjustment)
    {
        timeToPauseWorkerNanoSec += _curControlInfo.TbeTESAdjustment - controlInfo.TbeTESAdjustment;
    }

    // when reducing BatchTTL, worker need to wait out the delta (this should be rare)
    if (controlInfo.BatchTTL < _curControlInfo.BatchTTL)
    {
        timeToPauseWorkerNanoSec += _curControlInfo.BatchTTL - controlInfo.BatchTTL;
    }

    // when TbeNanoSecStep change(this should be really really rare if not an bug), sleep 1 microsecond if no other reason to sleep
    if (controlInfo.TbeNanoSecStep != _curControlInfo.TbeNanoSecStep &&
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
        K2INFO("AdjustWorker: worker core need to sleep(ns)" << std::to_string(timeToPauseWorkerNanoSec));
        // Get current time and compare with last request time to see how much more need to pause
        uint64_t curTimeMicroSecRounded = TSE_Count_MicroSecRounded(SysClock::now());

        if ((curTimeMicroSecRounded - timeToPauseWorkerNanoSec) < _lastRequestTimeMicrSecRounded)
        {
            // TODO: warning if sleep more than 10 microsecond
            auto sleepNanoSecCount = _lastRequestTimeMicrSecRounded + timeToPauseWorkerNanoSec - curTimeMicroSecRounded;
            K2INFO("Due to TSOWorkerControlInfo change, worker core:" << seastar::engine().cpu_id() << " going to sleep "<< sleepNanoSecCount << " nanosec.");
            if (sleepNanoSecCount >  10 * 1000)
            {
                K2WARN("TSOWorkerControlInfo change trigger long sleep. Worker core:" << seastar::engine().cpu_id() << " going to sleep "<< sleepNanoSecCount << " nanosec.");
            }

            // busy sleep
            while ((curTimeMicroSecRounded - timeToPauseWorkerNanoSec) < _lastRequestTimeMicrSecRounded)
            {
                curTimeMicroSecRounded = TSE_Count_MicroSecRounded(SysClock::now());
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

    //K2INFO("Start getting a timestamp batch");
    //K2INFO("Start getting a timestamp batch 2");

    // this function is on hotpath, code organized to optimized the most common happy case for efficiency

    // in most of time, it is happy path, where current time at microsecond level is greater than last call's time
    // i.e. each worker core has one call or less per microsecond
    // get current request time now, removing nano second part
    uint64_t nowMicroSecRounded = TSE_Count_MicroSecRounded(k2::SysClock::now());
    //K2INFO("Start getting a timestamp batch, got current time.");

    if (_curControlInfo.IsReadyToIssueTS &&
        nowMicroSecRounded + _curControlInfo.TbeTESAdjustment + 1000 < _curControlInfo.ReservedTimeShreshold &&
        nowMicroSecRounded > _lastRequestTimeMicrSecRounded)
    {
        uint16_t batchSizeToIssue = std::min(batchSizeRequested, (uint16_t)(1000/_curControlInfo.TbeNanoSecStep));

        result.TbeTESBase = nowMicroSecRounded + _curControlInfo.TbeTESAdjustment
            + seastar::engine().cpu_id() - 1;
        result.TSOId = _tsoId;
        result.TsDelta = _curControlInfo.TsDelta;
        result.TTLNanoSec = _curControlInfo.BatchTTL;
        result.TSCount = batchSizeToIssue;
        result.TbeNanoSecStep = _curControlInfo.TbeNanoSecStep;

        /*K2INFO("returning a tsBatch reqSize:" << batchSizeRequested << " at rounded request time:[" << nowMicroSecRounded
            << ":"<< _lastRequestTimeMicrSecRounded << "]TbeAdj:" << _curControlInfo.TbeTESAdjustment
            << " batch value(tbe:tsdelta:TTL:TSCount:Step)[" << result.TbeTESBase << ":" <<result.TsDelta << ":" << result.TTLNanoSec
            << ":" << batchSizeToIssue << ":" << result.TbeNanoSecStep <<"]");
        */

        _lastRequestTimeMicrSecRounded = nowMicroSecRounded;
        _lastRequestTimeStampCount = batchSizeToIssue;

        // TODO: accumulate statistics

        return result;
    }

    // otherwise, handle less frequent situation
    return GetTimeStampFromTSOLessFrequentHelper(batchSizeRequested, nowMicroSecRounded);
}

// helper function to issue timestamp (or check error situation)
TimestampBatch TSOService::TSOWorker::GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t nowMicroSecRounded)
{
    K2INFO("getting a timestamp batch in helper");
    // step 1/4 sanity check, check IsReadyToIssueTS and possible issued timestamp is within ReservedTimeShreshold
    if (!_curControlInfo.IsReadyToIssueTS)
    {
        K2WARN("Not ready to issue timestamp batch due to IsReadyToIssueTS, worker core:" << seastar::engine().cpu_id());

        // TODO: consider giving more detail information on why IsReadyToIssueTS is false, e.g. the instance is not master, not init, or wait/sleep
        throw TSONotReadyException();
    }

    // step 2/4 this is case when we try to issue timestamp batch beyond ReservedTimeShreshold (indicating it is not refreshed), this is really a bug and need to root cause.
    if (nowMicroSecRounded + _curControlInfo.TbeTESAdjustment + 1000 > _curControlInfo.ReservedTimeShreshold)
    {
        // this is really a bug if ReservedTimeShreshold is not updated promptly.
         K2WARN("Not ready to issue timestamp batch due to ReservedTimeShreshold, worker core:" << seastar::engine().cpu_id());

        // TODO: consider giving more detail information
        throw TSONotReadyException();
    }

    // step 3/4 if somehow current time is smaller than last request time
    if (nowMicroSecRounded < _lastRequestTimeMicrSecRounded)
    {
        // this is rare, normally should be a bug, add detal debug info later
        K2DEBUG("nowMicroSecRounded:" << nowMicroSecRounded << "< _lastRequestTimeMicrSecRounded:" <<_lastRequestTimeMicrSecRounded);
        // let client retry, maybe we should blocking sleep if there is a case this happening legit and the difference is small, e.g. a few microseconds
        throw TSONotReadyException();
    }

    // step 4/4 handle the case nowMicroSecRounded == _lastRequestTimeMicrSecRounded
    // If leftover timestamp of this microSec is sufficient, issue them out, otherwise, busy wait to next microsec and issue timestamp.
    K2ASSERT(nowMicroSecRounded == _lastRequestTimeMicrSecRounded, "last and this requests are in same microsecond!");
    uint16_t leftoverTS = 1000 / _curControlInfo.TbeNanoSecStep - _lastRequestTimeStampCount;

    if (leftoverTS < batchSizeRequested)
    {
        // not enough timestamp at current microsecond to issue out,
        // busy wait out and go through normal code path to issue timestamp on next microsecond
        while (nowMicroSecRounded == _lastRequestTimeMicrSecRounded)
        {
            nowMicroSecRounded = TSE_Count_MicroSecRounded(SysClock::now());
        }

        return GetTimestampFromTSO(batchSizeRequested);
    }

    K2ASSERT(nowMicroSecRounded == _lastRequestTimeMicrSecRounded, "last and this requests are in same microsecond!");

    TimestampBatch result;
    result.TbeTESBase = nowMicroSecRounded + _curControlInfo.TbeTESAdjustment
        + seastar::engine().cpu_id() - 1
        + _lastRequestTimeStampCount * _curControlInfo.TbeNanoSecStep;
    result.TSOId = _tsoId;
    result.TsDelta = _curControlInfo.TsDelta;
    result.TTLNanoSec = _curControlInfo.BatchTTL;
    result.TSCount = batchSizeRequested;
    result.TbeNanoSecStep = _curControlInfo.TbeNanoSecStep;

    //_lastRequestTimeMicrSecRounded = nowMicroSecRounded;
    _lastRequestTimeStampCount += batchSizeRequested; // we've just issued batchSizeRequested

    return result;
}

}
