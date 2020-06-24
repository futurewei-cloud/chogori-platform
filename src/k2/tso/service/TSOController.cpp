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
    K2INFO("TSOController start");

    K2ASSERT(!_stopRequested, "start during shutdown is not allowed!");

    // TODO: handle exception
    return InitializeInternal()
        .then([this] () mutable {return JoinServerCluster();})
        .then([this] (std::tuple<bool, uint64_t> joinResult) mutable { return SetRoleInternal(std::get<0>(joinResult), std::get<1>(joinResult)); })
        .then([this] () mutable {
            // set timers
            _heartBeatTimer.arm(_heartBeatTimerInterval());
            _timeSyncTimer.arm(_timeSyncTimerInterval());
            _statsUpdateTimer.arm(_statsUpdateTimerInterval());

            // register RPC APIs
            RegisterGetTSOMasterURL();
            RegisterGetTSOWorkersURLs();

            K2INFO("TSOController started");
            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::TSOController::gracefulStop() {
    K2INFO("TSOController stop");
    _stopRequested = true;

    // need to chain all operations into a sequencial future chain to have them done properly
    // gracefully wait all future done here
    return seastar::when_all_succeed(std::move(_heartBeatFuture), std::move(_timeSyncFuture), std::move(_statsUpdateFuture)).discard_result()
        .then([this] () mutable {
            _heartBeatTimer.cancel();
            _timeSyncTimer.cancel();
            _statsUpdateTimer.cancel();

            // unregistar all APIs
            RPC().registerMessageObserver(dto::Verbs::GET_TSO_MASTERSERVER_URL, nullptr);
            RPC().registerMessageObserver(dto::Verbs::GET_TSO_WORKERS_URLS, nullptr);
            // unregister internal APIs
            //RPC().registerMessageObserver(MsgVerbs::ACK, nullptr);

            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::TSOController::InitializeInternal()
{
    K2INFO("InitializeInternal");
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
    _lastSentControlInfo.TBENanoSecStep =   seastar::smp::count - 1;            // same as number of worker cores
    _lastSentControlInfo.TsDelta =          _defaultTBWindowSize().count();     // uncertain window size of timestamp from the batch is also default _defaultTBWindowSize
    _lastSentControlInfo.BatchTTL =         _defaultTBWindowSize().count();     // batch's TTL is also _defaultTBWindowSize

    _controlInfoToSend.TBENanoSecStep =     seastar::smp::count - 1;            
    _controlInfoToSend.TsDelta =            _defaultTBWindowSize().count();
    _controlInfoToSend.BatchTTL =           _defaultTBWindowSize().count();
}

seastar::future<> TSOService::TSOController::GetAllWorkerURLs()
{
    K2INFO("GetAllWorkerURLs");
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
                K2ASSERT(singleRes[0].size() > 0, "Invalid worker URLs");
                result.insert(result.end(), singleRes.begin(), singleRes.end());
            }
            return result;
        })
    .then([this] (std::vector<std::vector<k2::String>>&& result) mutable
    {
        _workersURLs = std::move(result);
        K2INFO("got workerURLs:" <<_workersURLs.size());
        return seastar::make_ready_future<>();
    });
}

seastar::future<> TSOService::TSOController::SetRoleInternal(bool isMaster, uint64_t prevReservedTimeShreshold)
{
    K2INFO("SetRoleInternal isMaster:" << std::to_string(isMaster));
    if (!_isMasterInstance && isMaster)  // change from standby to master
    {
        // when change from standby to master
        // we need to immediately set the _isMasterInstance flag and _prevReservedTimeShreshold (so current regular heartbeat and SendWorkersControlInfo will pick latest change)
        // then issue out of band heartBeat, to make our own TimeShreshold reservation and start service immediately afterwards.
        // If prevReservedTimeShreshold is in the future, we need to wait out that time before issue out of band heartBeat
        _prevReservedTimeShreshold = prevReservedTimeShreshold;
        _isMasterInstance = isMaster; // true

        // If prevReservedTimeShreshold is in the past, or within next heartbeat cycle,
        // then issue out of band heartBeat, to make our own TimeShreshold reservation and start service immediately afterwards,
        // otherwise, let regular hearBeat to pick up the work.
        uint64_t curTimeTSECount = TimeAuthorityNow();

        if (prevReservedTimeShreshold < curTimeTSECount)
        {
            // we do not need to hold as prevReservedTimeShreshold is past
            return DoHeartBeat();
        }
        else if (prevReservedTimeShreshold - curTimeTSECount < (uint64_t) _heartBeatTimerInterval().count())
        {
            std::chrono::nanoseconds sleepDur(prevReservedTimeShreshold - curTimeTSECount);
            return seastar::sleep(sleepDur).then([this]
            {
                return DoHeartBeat();
            });
        }

        // let regular heartbeat to deal with _prevReservedTimeShreshold
        return seastar::make_ready_future<>();
    }
    else if (_isMasterInstance && !isMaster) // change from master to standby
    {
        // set the _isMasterInstance to false, and update worker
        _isMasterInstance = false;
        // reuse latest control info,  IsReadyToIssueTS will be set inside SendWorkersControlInfo()
        _controlInfoToSend =  _lastSentControlInfo;

        return SendWorkersControlInfo();
    }
    else if (_isMasterInstance && isMaster)
    {
        // why we are doing this noop, is this a bug? let it crash in debug mode
        K2ASSERT(false, "Noop update from master to master!");
        K2WARN("Noop update from master to master!");
        return seastar::make_ready_future<>();
    }
    else // !_isMasterInstance && !isMaster
    {
        // why we are doing this noop, is this a bug? let it crash in debug mode
        K2ASSERT(false, "Noop update from standby to standby!");
        K2WARN("Noop update from standby to standby!");
        return seastar::make_ready_future<>();
    }
};

void TSOService::TSOController::RegisterGetTSOMasterURL()
{
    k2::RPC().registerMessageObserver(dto::Verbs:: GET_TSO_MASTERSERVER_URL, [this](k2::Request&& request) mutable
    {
        auto response = request.endpoint.newPayload();
        K2INFO("Master TSO TCP endpoint is: " << _masterInstanceURL);
        response->write((void*)_masterInstanceURL.c_str(), _masterInstanceURL.size());
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

    if (_isMasterInstance)
    {
        uint64_t curTimeTSECount = TimeAuthorityNow();

        // case 1, if we lost lease, suicide now
        if (curTimeTSECount >  _myLease)
        {
            K2INFO("Lost lease detected during HeartBeat. cur time and mylease : " << curTimeTSECount << ":" << _myLease);
            //K2ASSERT(false, "Lost lease detected during HeartBeat.");
            //Suicide();
        }

        // case 2, if prevReservedTimeShreshold is in future and within one heartbeat, sleep out and recursive call DoHeartBeat()
        // prevReservedTimeShreshold - curTimeTSECount < _heartBeatTimerInterval().count()
        if (_prevReservedTimeShreshold > curTimeTSECount &&
            (_prevReservedTimeShreshold - curTimeTSECount < (uint64_t) _heartBeatTimerInterval().count()))
        {
            std::chrono::nanoseconds sleepDur(_prevReservedTimeShreshold - curTimeTSECount);
            return seastar::sleep(sleepDur).then([this]
            {
                return DoHeartBeat();
            });
        }

        // case 3, if prevReservedTimeShreshold is in future and beyond one heartbeat, send heartbeat with renew lease only
        if (_prevReservedTimeShreshold > curTimeTSECount &&
            (_prevReservedTimeShreshold - curTimeTSECount >= (uint64_t) _heartBeatTimerInterval().count()))
        {
            return RenewLeaseOnly().then([this](uint64_t newLease) {_myLease = newLease;});
        }

        // case 4, regular situation, extending lease and ReservedTimeThreshold, then SendWorkersControlInfo
        return RenewLeaseAndExtendReservedTimeThreshold()
            .then([this] (std::tuple<uint64_t, uint64_t> newLeaseAndThreshold) mutable {
                // set new lease and new threshold
                _myLease = std::get<0>(newLeaseAndThreshold);
                _controlInfoToSend.ReservedTimeShreshold = std::get<1>(newLeaseAndThreshold);

                uint64_t newCurTimeTSECount = TimeAuthorityNow();
                K2ASSERT(_controlInfoToSend.ReservedTimeShreshold > newCurTimeTSECount && _myLease > newCurTimeTSECount,
                    "new lease and ReservedTimeThreshold should be in the future.");

                // update worker!
                return SendWorkersControlInfo();
            });
    }
    else
    {
        return UpdateStandByHeartBeat()
            .then([this] () mutable {
                return SendWorkersControlInfo();
            });
    }
}

seastar::future<> TSOService::TSOController::DoHeartBeatDuringStop()
{
    K2ASSERT(_stopRequested, "Why are we here when stop is not requested?");

    if (!_isMasterInstance)
    {
        // we no longer need to send heart beat as we were standby and are stopping
        K2ASSERT(_lastSentControlInfo.IsReadyToIssueTS == false, "workers should not be in issuing TS state!");
        return seastar::make_ready_future<>();
    }

    // now we are master instance and stopping

    uint64_t curTimeTSECount = TimeAuthorityNow();

    if (_prevReservedTimeShreshold > curTimeTSECount)
    {
        // we should not yet enable workers
        K2ASSERT(_lastSentControlInfo.IsReadyToIssueTS == false, "workers should not be in issuing TS state!");

        // set no longer master.
        _isMasterInstance = false;

        // remove our lease on Paxos
        return RemoveLeaseFromPaxos();
    }

    // set _isMasterInstance to false send to workers first to stop issuing timestamp
    // and then nicely reduce ReservedTimeShreshold to new currentTime + _lastSentControlInfo.TBEAdjustment and remove lease
    // so that other standby can quickly become new master
    _isMasterInstance = false;

    return SendWorkersControlInfo()
        .then([this] () mutable {
            //uint64_t newReservedTimeShresholdTSECount = now_nsec_count()
            //    + std::max(_lastSentControlInfo.TBEAdjustment, (uint64_t) _defaultTBWindowSize().count());

            // remove our lease on Paxos and update ReservedTimeShreshold
            return RemoveLeaseFromPaxosWithUpdatingReservedTimeShreshold(/*newReservedTimeShresholdTSECount*/);
        });
}

// really send the _controlInfoToSend to worker, and only place to set IsReadyToIssueTS inside _controlInfoToSend based on current state
seastar::future<> TSOService::TSOController::SendWorkersControlInfo()
{
    // step 1/3 decide IsReadyToIssueTS should be true or not
    bool readyToIssue = false;
    if (_isMasterInstance)
    {
        uint64_t curTimeTSECount = TimeAuthorityNow();

        // if lost lease, suicide.
        if(curTimeTSECount > _myLease)
        {
            Suicide();
        }

        // worker can only issue TS under following condition
        if (!_stopRequested &&                                              // stop is not requested
            _prevReservedTimeShreshold < curTimeTSECount &&                 // _prevReservedTimeShreshold is in the past
            _controlInfoToSend.ReservedTimeShreshold > curTimeTSECount )    // new ReservedTimeShreshold is in the future
        {
            readyToIssue = true;
        }
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
    K2ASSERT(_isMasterInstance && curTime > _myLease, "Suicide when not lost lease or not master?");

    K2ASSERT(false, "Suiciding");
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

            K2ASSERT(uncertaintyWindowSize < 3000, "trueTime windows size should be less than 3us.");

            if (_diffTALocalInNanosec == 0)
            {
                K2INFO("First TimeSyncd - deltaToSteadyClock value:" << deltaToSteadyClock << " uncertaintyWindowSize:" << uncertaintyWindowSize);
            }

            if (!_isMasterInstance)
            {
                _diffTALocalInNanosec = deltaToSteadyClock;
                // as localNow is always in TrueTime window, these values will be kept as initialized
                // The batch uncertainty window size is _defaultTBWindowSize, assuming _defaultTBWindowSize > uncertaintyWindowSize
                _lastSentControlInfo.TBEAdjustment  = deltaToSteadyClock + _defaultTBWindowSize().count() - uncertaintyWindowSize / 2;     
                _lastSentControlInfo.TsDelta        = _defaultTBWindowSize().count();    // batch window size is also default _defaultTBWindowSize
                _controlInfoToSend.TBEAdjustment    = deltaToSteadyClock + _defaultTBWindowSize().count() - uncertaintyWindowSize / 2;     
                _controlInfoToSend.TsDelta          = _defaultTBWindowSize().count();
            }
            else // master case
            {
                // local steady_clock drifted from time authority or time authority changed time for more than 1000 nanosecond or 1 microsecond, need to do smearing adjustment
                uint64_t driftDelta = std::abs(((int64_t) _diffTALocalInNanosec) - ((int64_t) deltaToSteadyClock));
                if (driftDelta > 1000)
                {
                    if (_diffTALocalInNanosec != 0)
                    {
                        // TODO adjust TBEAdjustment with smearing as well, at the rate less than 1- (TsDelta/MTL) instad of direct one time change.
                        K2WARN("Local steady_clock drift away from Time Authority! Smearing adjustment in progress. old diff value:" << _diffTALocalInNanosec << "new diff value:" << deltaToSteadyClock);
                    }
                    _diffTALocalInNanosec = deltaToSteadyClock;
                    // The batch uncertainty window size is _defaultTBWindowSize, assuming _defaultTBWindowSize > uncertaintyWindowSize
                    _controlInfoToSend.TBEAdjustment =   deltaToSteadyClock + _defaultTBWindowSize().count() - uncertaintyWindowSize / 2;
                }
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
            K2WARN("diff between sys time and steady time changed from "<<_diffTALocalInNanosec <<" to " << newDiffTALocalInNanosec << " ignored !");
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
