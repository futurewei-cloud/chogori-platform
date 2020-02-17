#include <algorithm>    // std::min
#include "seastar/core/sleep.hh"

#include <k2/common/Log.h>
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include "TSOService.h"
#include "MessageVerbs.h"

namespace k2 
{

seastar::future<> TSOService::TSOWorker::start() 
{
    _tsoId = _outer.TSOId(); 

    RegisterGetTSOTimeStampBatch();
    return seastar::make_ready_future<>();
}

seastar::future<> TSOService::TSOWorker::stop() 
{
    // unregistar all APIs
    RPC().registerMessageObserver(TSOMsgVerbs::GET_TSO_TIMESTAMP_BATCH, nullptr);
    return seastar::make_ready_future<>();
}

void RegisterGetTSOTimeStampBatch();
{
    k2::RPC().registerMessageObserver(TSOMsgVerbs::GET_TSO_TIMESTAMP_BATCH, [this](k2::Request&& request) mutable 
    {
        // TODO: handle exceptions
        auto response = request.endpoint.newPayload();
        auto timeStampBatch = GetTimeStampFromTSO();
        //K2INFO("time stamp batch returned is: " << timeStampBatch);
        response->write(timeStampBatch);
        k2::RPC().sendReply(std::move(response), request);
    });
}

seastar::future<> TSOService::TSOWorker::UpdateWorkerControlInfo(TSOWorkerControlInfo controlInfo)
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

    return seastar::make_ready_future<>(); 
}

void TSOService::TSOWorker::AdjustWorker(const TSOWorkerControlInfo& controlInfo)
{    
    K2INFO("AdjustWorker: worker core:" << seastar::engine().cpu_id());

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

// API issuing TimeStamp to the TSO client
TimeStampBatch TSOService::TSOWorker::GetTimeStampFromTSO(uint16_t batchSizeRequested) 
{
    TimeStampBatch result;

    // this function is on hotpath, code organized to optimized the most common happy case for efficiency
    
    // in most of time, it is happy path, where current time at microsecond level is greater than last call's time
    // i.e. each worker core has one call or less per microsecond
    // get current request time now, removing nano second part
    uint64_t nowMicroSecRounded = TSE_Count_MicroSecRounded(k2::SysClock::now());
    
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

        _lastRequestTimeMicrSecRounded = nowMicroSecRounded;
        _lastRequestTimeStampCount = batchSizeToIssue;

        // TODO: accumulate statistics

        return result;
    }

    // otherwise, handle less frequent situation
    return GetTimeStampFromTSOLessFrequentHelper(batchSizeRequested, nowMicroSecRounded);
}

// helper function to issue timeStamp (or check error situation)
TimeStampBatch TSOService::TSOWorker::GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t nowMicroSecRounded) 
{
    // step 1/4 sanity check, check IsReadyToIssueTS and possible issued timestamp is within ReservedTimeShreshold 
    if (!_curControlInfo.IsReadyToIssueTS)
    {
        K2WARN("Not ready to issue timestamp batch due to IsReadyToIssueTS, worker core:" << seastar::engine().cpu_id());

        // TODO: consider giving more detail information on why IsReadyToIssueTS is false, e.g. the instance is not master, not init, or wait/sleep 
        throw ServerNotReadyException();
    }

    // step 2/4 this is case when we try to issue timestamp batch beyond ReservedTimeShreshold (indicating it is not refreshed), this is really a bug and need to root cause.
    if (nowMicroSecRounded + _curControlInfo.TbeTESAdjustment + 1000 > _curControlInfo.ReservedTimeShreshold)
    {
        // this is really a bug if ReservedTimeShreshold is not updated promptly.
         K2WARN("Not ready to issue timestamp batch due to ReservedTimeShreshold, worker core:" << seastar::engine().cpu_id());

        // TODO: consider giving more detail information
        throw ServerNotReadyException();
    }

    // step 3/4 if somehow current time is smaller than last request time
    if (nowMicroSecRounded < _lastRequestTimeMicrSecRounded)
    {
        // this is rare, normally should be a bug, add detal debug info later
        K2DEBUG("nowMicroSecRounded:" << nowMicroSecRounded << "< _lastRequestTimeMicrSecRounded:" <<_lastRequestTimeMicrSecRounded);
        // let client retry, maybe we should blocking sleep if there is a case this happening legit and the difference is small, e.g. a few microseconds
        throw ServerNotReadyException();
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

        return GetTimeStampFromTSO(batchSizeRequested);
    }

    K2ASSERT(nowMicroSecRounded == _lastRequestTimeMicrSecRounded, "last and this requests are in same microsecond!");

    TimeStampBatch result;
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