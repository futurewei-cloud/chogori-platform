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

#include <algorithm>    // std::min/max
#include <tuple>

#include <boost/range/irange.hpp>

#include "seastar/core/sleep.hh"

#include <k2/common/Log.h>
#include <k2/config/Config.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include "TSOService.h"

namespace k2
{
seastar::future<> TSOService::TSOController::start()
{
    K2LOG_I(log::tsoserver, "TSOController start");

    K2ASSERT(log::tsoserver, !_stopRequested, "start during shutdown is not allowed!");

    // TODO: handle exception
    return InitializeInternal()
        .then([this] () mutable {return JoinServerCluster();})
        .then([this] () mutable {
            // set timers
            _heartBeatTimer.arm(_heartBeatTimerInterval());
            _timeSyncTimer.arm(_timeSyncTimerInterval());
            _statsUpdateTimer.arm(_statsUpdateTimerInterval());

            // register RPC APIs
            RegisterGetTSOServerURLs();
            RegisterGetTSOWorkersURLs();

            K2LOG_I(log::tsoserver, "TSOController started");
            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::TSOController::gracefulStop() {
    K2LOG_I(log::tsoserver, "TSOController stop");
    _stopRequested = true;

    // need to chain all operations into a sequencial future chain to have them done properly
    // gracefully wait all future done here
    return seastar::when_all_succeed(std::move(_heartBeatFuture), std::move(_timeSyncFuture), std::move(_statsUpdateFuture)).discard_result()
        .then([this] () mutable {
            _heartBeatTimer.cancel();
            _timeSyncTimer.cancel();
            _statsUpdateTimer.cancel();

            // unregistar all APIs
            RPC().registerMessageObserver(dto::Verbs::GET_TSO_SERVER_URLS, nullptr);
            RPC().registerMessageObserver(dto::Verbs::GET_TSO_WORKERS_URLS, nullptr);
            // unregister internal APIs
            //RPC().registerMessageObserver(MsgVerbs::ACK, nullptr);

            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::TSOController::InitializeInternal()
{
    K2LOG_I(log::tsoserver, "InitializeInternal");
    // step 1/3 initialize worker control info
    InitWorkerControlInfo();

    // step 2/3 TimeSync with Atomic/GPS clock
    // step 3/3 Gather workers URLs
    return DoTimeSync()
        .then([this] () mutable {return GetAllWorkerURLs();});
}

void TSOService::TSOController::InitWorkerControlInfo()
{
    
    // initialize TSOWorkerControlInfo
    _lastSentControlInfo.IgnoreThreshold =  _ignoreReservedTimeThreshold();
    _lastSentControlInfo.TBENanoSecStep =   seastar::smp::count - 1;            // same as number of worker cores
    _lastSentControlInfo.TsDelta =          _defaultTBWindowSize().count();     // uncertain window size of timestamp from the batch is also default _defaultTBWindowSize
    _lastSentControlInfo.BatchTTL =         _defaultTBWindowSize().count();     // batch's TTL is also _defaultTBWindowSize

    _controlInfoToSend.IgnoreThreshold =    _ignoreReservedTimeThreshold();
    K2LOG_I(log::tsoserver, "InitWorkerControlInfo, IgnoreThreshold:{}", _controlInfoToSend.IgnoreThreshold);
    _controlInfoToSend.TBENanoSecStep =     seastar::smp::count - 1;
    _controlInfoToSend.TsDelta =            _defaultTBWindowSize().count();
    _controlInfoToSend.BatchTTL =           _defaultTBWindowSize().count();
}

seastar::future<> TSOService::TSOController::GetAllWorkerURLs()
{
    K2LOG_I(log::tsoserver, "GetAllWorkerURLs");
    return seastar::map_reduce(boost::irange(1u, seastar::smp::count),   // all worker cores, starting from 1
        [this] (unsigned cpuId) {
            return AppBase().getDist<k2::TSOService>().invoke_on(cpuId, &TSOService::GetWorkerURLs)
            .then([] (std::vector<k2::String>&& urls) {
                std::vector<std::vector<k2::String>> wrapped_urls;
                wrapped_urls.emplace_back(std::move(urls));
                return seastar::make_ready_future<std::vector<std::vector<k2::String>>>(std::move(wrapped_urls));
            });
        },
        std::vector<std::vector<k2::String>>(),
        [] (std::vector<std::vector<k2::String>>&& singleRes, std::vector<std::vector<k2::String>>&& result) {
            if (!singleRes.empty())
            {
                // just check first one for simplicity, ideally, should check all returned work core url has value.
                K2ASSERT(log::tsoserver, singleRes[0].size() > 0, "Invalid worker URLs");
                result.insert(result.end(), singleRes.begin(), singleRes.end());
            }
            return result;
        })
    .then([this] (std::vector<std::vector<k2::String>>&& result) mutable
    {
        _workersURLs = std::move(result);
        K2LOG_I(log::tsoserver, "got workerURLs: {}", _workersURLs);
        return seastar::make_ready_future<>();
    });
}

/*
   GET_TSO_SERVER_URLS

   TSO servers are a group of independent servers connected to its own TimeAuthority/AtomicClock
   This API allow client to get all currently live TSO servers from any one of them. 
   TSO client is expected to start with a cmd option prividing a subset of TSO servers and update the list later. 
   TODO:
      1. implment client side logic of updating the server list by calling this API.
      2. TSO servers could form a gossip cluster, to discovery each other and more rarely to detech if anyone's 
         TimeAuthroity/AtomicClock is out of band. 
 
*/
void TSOService::TSOController::RegisterGetTSOServerURLs()
{
    k2::RPC().registerMessageObserver(dto::Verbs:: GET_TSO_SERVER_URLS, [this](k2::Request&& request) mutable
    {
        auto response = request.endpoint.newPayload();
        response->write(_TSOServerURLs);
        K2LOG_D(log::tsoserver, "returned TSO Server TCP endpoints are: {}", _TSOServerURLs);
        return k2::RPC().sendReply(std::move(response), request);
    });
}

void TSOService::TSOController::RegisterGetTSOWorkersURLs()
{
    k2::RPC().registerMessageObserver(dto::Verbs::GET_TSO_WORKERS_URLS, [this](k2::Request&& request) mutable
    {
        auto response = request.endpoint.newPayload();
        response->write(_workersURLs);
        return k2::RPC().sendReply(std::move(response), request);
    });
}

void TSOService::TSOController::HeartBeat()
{
    // delayed heartbeat detection and logging
    uint64_t nowCnt = now_nsec_count();

    if ((nowCnt - _lastHeartBeat) > (uint64_t)( 2 * (nsec(_heartBeatTimerInterval()).count())))
    {
        if (_lastHeartBeat != 0)
        {
            K2LOG_D(log::tsoserver, "HeartBeat delayed more than two preset intervals, _lastHeartBeat:{}, preset duration in nano sec: {}",
                _lastHeartBeat,  nsec(_heartBeatTimerInterval()).count());
        }
    }
    _lastHeartBeat = nowCnt;
    // end of delayed heart beat detection and logging

    K2LOG_D(log::tsoserver, "HeartBeat triggered");
    _heartBeatFuture = DoHeartBeat()
        .then([this] () mutable
        {
            if (!_stopRequested)
            {
                _heartBeatTimer.arm(_heartBeatTimerInterval());
            }
        });

    return;
}

seastar::future<> TSOService::TSOController::DoHeartBeat()
{
    if (_stopRequested)
    {
        return DoHeartBeatDuringStop();
    }

    uint64_t curTimeTSECount = TimeAuthorityNow();

    // case 1, if we lost lease, suicide now
    if (curTimeTSECount >  _myLease)
    {
        K2LOG_D(log::tsoserver, "Lost lease detected during HeartBeat. cur time and mylease : {}:{}",curTimeTSECount, _myLease);
        //K2ASSERT(log::tsoserver, false, "Lost lease detected during HeartBeat.");
        //Suicide();
    }

    // case 2, if prevReservedTimeShreshold is in future and within one heartbeat, sleep out and recursive call DoHeartBeat()
    // prevReservedTimeShreshold - curTimeTSECount < _heartBeatTimerInterval().count()
    if (_prevReservedTimeShreshold > curTimeTSECount &&
        (_prevReservedTimeShreshold - curTimeTSECount < (uint64_t) _heartBeatTimerInterval().count()))
    {
        K2LOG_D(log::tsoserver, "_prevReservedTimeShreshold is one heardbead away to expire, sleep over it and do HeartBeat again");
        std::chrono::nanoseconds sleepDur(_prevReservedTimeShreshold - curTimeTSECount);
        return seastar::sleep(sleepDur).then([this]
        {
            K2LOG_D(log::tsoserver, "_prevReservedTimeShreshold was one heardbead away to expire, slept over it and about to HeartBeat again");
            return DoHeartBeat();
        });
    }

    // case 3, if prevReservedTimeShreshold is in future and beyond one heartbeat, send heartbeat with renew lease only
    if (_prevReservedTimeShreshold > curTimeTSECount &&
        (_prevReservedTimeShreshold - curTimeTSECount >= (uint64_t) _heartBeatTimerInterval().count()))
    {
        K2LOG_D(log::tsoserver, "_prevReservedTimeShreshold is in the future and beyong one heardbead, just renew lease without SendWorkerControlInfo");
        return RenewLeaseOnly().then([this](uint64_t newLease) {_myLease = newLease;});
    }

    // case 4, regular situation, extending lease and ReservedTimeThreshold, then SendWorkersControlInfo
    return RenewLeaseAndExtendReservedTimeThreshold()
        .then([this] (std::tuple<uint64_t, uint64_t> newLeaseAndThreshold) mutable {
            // set new lease and new threshold
            _myLease = std::get<0>(newLeaseAndThreshold);
            _controlInfoToSend.ReservedTimeThreshold = std::get<1>(newLeaseAndThreshold);

            if (!_controlInfoToSend.IgnoreThreshold)
            {
                uint64_t newCurTimeTSECount = TimeAuthorityNow();
                K2ASSERT(log::tsoserver, _controlInfoToSend.ReservedTimeThreshold > newCurTimeTSECount && _myLease > newCurTimeTSECount,
                    "new lease and ReservedTimeThreshold should be in the future.");
            }

            K2LOG_D(log::tsoserver, "SendWorkersControlInfo during regular HeartBeat. new ReservedTimeThreshold:{}", _controlInfoToSend.ReservedTimeThreshold);
            // update worker!
            return SendWorkersControlInfo();
        });
}


seastar::future<> TSOService::TSOController::DoHeartBeatDuringStop()
{
    K2ASSERT(log::tsoserver, _stopRequested, "Why are we here when stop is not requested?");

    uint64_t curTimeTSECount = TimeAuthorityNow();

    if (_prevReservedTimeShreshold > curTimeTSECount)
    {
        // we should not yet enable workers
        K2ASSERT(log::tsoserver, _lastSentControlInfo.IsReadyToIssueTS == false, "workers should not be in issuing TS state!");

        // remove our lease on Paxos
        return RemoveLeaseFromPaxos();
    }

    return SendWorkersControlInfo()
        .then([this] () mutable {
            //uint64_t newReservedTimeShresholdTSECount = now_nsec_count()
            //    + std::max(_lastSentControlInfo.TBEAdjustment, (uint64_t) _defaultTBWindowSize().count());

            // remove our lease on Paxos and update ReservedTimeThreshold
            return RemoveLeaseFromPaxosWithUpdatingReservedTimeShreshold(/*newReservedTimeShresholdTSECount*/);
        });
}

// really send the _controlInfoToSend to worker, and only place to set IsReadyToIssueTS inside _controlInfoToSend based on current state
seastar::future<> TSOService::TSOController::SendWorkersControlInfo()
{
    // step 1/3 decide IsReadyToIssueTS should be true or not
    bool readyToIssue = false;
    uint64_t curTimeTSECount = TimeAuthorityNow();

    // if lost lease, suicide.
    if(curTimeTSECount > _myLease)
    {
        Suicide();
    }

    // worker can only issue TS under following condition
    if (!_stopRequested &&                                              // stop is not requested
        (_controlInfoToSend.ReservedTimeThreshold > curTimeTSECount || _controlInfoToSend.IgnoreThreshold))    // new ReservedTimeThreshold is in the future OR it is ignored
    {
        readyToIssue = true;
    }

    _controlInfoToSend.IsReadyToIssueTS = readyToIssue;

    // step 2/3 update _lastSentControlInfo
    _lastSentControlInfo = _controlInfoToSend;

    // step 3/3 submit to workers
    auto& dist = AppBase().getDist<k2::TSOService>();
    return dist.invoke_on_others(
        [info=_controlInfoToSend] (auto& worker) {
            worker.UpdateWorkerControlInfo(info);
        });
}

void TSOService::TSOController::Suicide()
{
    // suicide when and only when we are master and find we lost lease
    auto curTime = TimeAuthorityNow();
    K2ASSERT(log::tsoserver, curTime > _myLease, "Suicide when not lost lease?");
    K2LOG_F(log::tsoserver, "TSO suicide requested!");
    K2ASSERT(log::tsoserver, false, "Suiciding");
    std::terminate();
}

void TSOService::TSOController::TimeSync()
{
    // timesync task will do nothing when _stopRequested
    if (_stopRequested)
        return;

    _timeSyncFuture = DoTimeSync().then( [this] () mutable
    {
        if (!_stopRequested)
        {
            _timeSyncTimer.arm(_timeSyncTimerInterval());
        }
    });

    return;
}

seastar::future<> TSOService::TSOController::DoTimeSync()
{
    // timesync task will do nothing when _stopRequested
    if (_stopRequested)
        return seastar::make_ready_future<>();

    return CheckAtomicGPSClock()
        .then([this](std::tuple<uint64_t, uint64_t> tt /*truetime result from CheckAtomicGPSClock() in the form of <steady_clock delta, uncertainty window size>*/ ) mutable {
            if (_stopRequested)
                return seastar::make_ready_future<>();

            auto& [deltaToSteadyClock, uncertaintyWindowSize] = tt;

            K2ASSERT(log::tsoserver, uncertaintyWindowSize < 3000, "trueTime windows size should be less than 3us.");

            if (_diffTALocalInNanosec == 0)
            {
                K2LOG_I(log::tsoserver, "First TimeSyncd - deltaToSteadyClock value: {}, uncertaintyWindowSize: {}", deltaToSteadyClock, uncertaintyWindowSize);
            }

            // local steady_clock drifted from time authority or time authority changed time for more than 1000 nanosecond or 1 microsecond, need to do smearing adjustment
            uint64_t driftDelta = std::abs(((int64_t) _diffTALocalInNanosec) - ((int64_t) deltaToSteadyClock));
            if (driftDelta > 1000)
            {
                if (_diffTALocalInNanosec != 0)
                {
                    // TODO adjust TBEAdjustment with smearing as well, at the rate less than 1- (TsDelta/MTL) instad of direct one time change.
                    K2LOG_W(log::tsoserver, "Local steady_clock drift away from Time Authority! Smearing adjustment in progress. old diff value: {}, new diff value: {}", _diffTALocalInNanosec, deltaToSteadyClock);
                }
                _diffTALocalInNanosec = deltaToSteadyClock;
                // The batch uncertainty window size is _defaultTBWindowSize, assuming _defaultTBWindowSize > uncertaintyWindowSize
                _controlInfoToSend.TBEAdjustment =   deltaToSteadyClock + _defaultTBWindowSize().count() - uncertaintyWindowSize / 2;
            }

            return seastar::make_ready_future<>();
        });
}

seastar::future<std::tuple<uint64_t, uint64_t>> TSOService::TSOController::CheckAtomicGPSClock()
{
    //TODO: implement this
    // fake time diff of Atomic clock to local steady clock with that between system_clock and steady_clock
    uint64_t newDiffTALocalInNanosec = sys_now_nsec_count() - now_nsec_count();

    // there is lots of noise for newDiffTALocalInNanosec, fake removing the noise
    if (_diffTALocalInNanosec!= 0)
    {
        /*
        uint64_t driftDelta =  std::abs(_diffTALocalInNanosec, newDiffTALocalInNanosec);
        if (driftDelta > 10 * 1000) // warn if diff drift more than 10 microsecond
        {
            K2LOG_W(log::tsoserver, "diff between sys time and steady time changed from "<<_diffTALocalInNanosec <<" to " << newDiffTALocalInNanosec << " ignored !");
        }*/
        // always ignore the diff.
        newDiffTALocalInNanosec = _diffTALocalInNanosec;
    }

    std::tuple<uint64_t, uint64_t> fakeResult(newDiffTALocalInNanosec, 2000/*fake 2000 nanosec uncertainty window size*/);
    return seastar::make_ready_future<std::tuple<uint64_t, uint64_t>>(fakeResult);
}

void TSOService::TSOController::CollectAndReportStats()
{
    _statsUpdateFuture = DoCollectAndReportStats().then( [this] () mutable
    {
        if (!_stopRequested)
        {
            _statsUpdateTimer.arm(_timeSyncTimerInterval());
        }
    });

    return;
}

seastar::future<> TSOService::TSOController::DoCollectAndReportStats()
{
    return seastar::make_ready_future<>();
}

}
