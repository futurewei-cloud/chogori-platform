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

#include <k2/common/Chrono.h>
#include <k2/common/Log.h>
#include <k2/config/Config.h>
#include <k2/transport/RPCDispatcher.h>  // for RPC

#include <algorithm>  // std::min/max
#include <boost/range/irange.hpp>
#include <tuple>

#include "TSOService.h"
#include "seastar/core/sleep.hh"

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
            _timeSyncTimer.arm(_timeSyncTimerInterval());
            _clusterGossipTimer.arm(_clusterGossipTimerInterval());
            _statsUpdateTimer.arm(_statsUpdateTimerInterval());

            // register RPC APIs
            RPC().registerRPCObserver<dto::GetTSOServerURLsRequest, dto::GetTSOServerURLsResponse>
            (dto::Verbs::GET_TSO_SERVER_URLS, [this](dto::GetTSOServerURLsRequest&& request) {
                return handleGetTSOServerURLs(std::move(request));
            });
            RPC().registerRPCObserver<dto::GetTSOServiceNodeURLsRequest, dto::GetTSOServiceNodeURLsResponse>
            (dto::Verbs::GET_TSO_SERVICE_NODE_URLS, [this](dto::GetTSOServiceNodeURLsRequest&& request) {
                return handleGetTSOServiceNodeURLs(std::move(request));
            });

            K2LOG_I(log::tsoserver, "TSOController started");
            return seastar::make_ready_future<>();
        });
}

seastar::future<> TSOService::TSOController::gracefulStop() {
    K2LOG_I(log::tsoserver, "TSOController stop");
    _stopRequested = true;

    // need to chain all operations into a sequencial future chain to have them done properly
    // gracefully wait all future done here
    return seastar::when_all_succeed(std::move(_timeSyncFuture), std::move(_clusterGossipFuture), std::move(_statsUpdateFuture)).discard_result()
        .then([this] () mutable {
            _timeSyncTimer.cancel();
            _clusterGossipTimer.cancel();
            _statsUpdateTimer.cancel();

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
    uint16_t tbWindow = std::min((uint16_t)nsec(_defaultTBWindowSize()).count(), (uint16_t)(1 << 16));
    // initialize TSOWorkerControlInfo
    _lastSentControlInfo.IgnoreThreshold =  _ignoreReservedTimeThreshold();
    _lastSentControlInfo.TBENanoSecStep =   seastar::smp::count - 1; // same as number of worker cores
    _lastSentControlInfo.TsDelta = tbWindow; // uncertain window size of timestamp from the batch is also default _defaultTBWindowSize
    _lastSentControlInfo.BatchTTL = tbWindow; // batch's TTL is also _defaultTBWindowSize

    _controlInfoToSend.IgnoreThreshold =    _ignoreReservedTimeThreshold();
    K2LOG_I(log::tsoserver, "InitWorkerControlInfo, IgnoreThreshold:{}", _controlInfoToSend.IgnoreThreshold);
    _controlInfoToSend.TBENanoSecStep =     seastar::smp::count - 1;
    _controlInfoToSend.TsDelta = tbWindow;
    _controlInfoToSend.BatchTTL = tbWindow;
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

   TSO server pool are a group of independent TSO servers/nodes connected to its own TimeAuthority/AtomicClock.
   This API allow client to get all currently live TSO servers/nodes from any one of them.
   TSO client is expected to start with a cmd option prividing a subset of TSO servers and update the list later.
   TODO:
      1. implement client side logic of updating the server list by calling this API.
      2. TSO servers could form a gossip cluster, to discovery each other and more rarely to detect if anyone's
         TimeAuthority/AtomicClock is out of band.
*/
seastar::future<std::tuple<Status, dto::GetTSOServerURLsResponse>>
TSOService::TSOController::handleGetTSOServerURLs(dto::GetTSOServerURLsRequest&& request)
{
    K2LOG_D(log::tsoserver, "handleGetTSOServerURLs");
    (void) request;

    GetTSOServerURLsResponse response{.serverURLs = _TSOServerURLs};
    K2LOG_D(log::tsoserver, "returned TSO Server TCP endpoints are: {}", _TSOServerURLs);
    return RPCResponse(Statuses::S200_OK("OK"), std::move(response));
}

seastar::future<std::tuple<Status, dto::GetTSOServiceNodeURLsResponse>>
TSOService::TSOController::handleGetTSOServiceNodeURLs(dto::GetTSOServiceNodeURLsRequest&& request)
{
    K2LOG_D(log::tsoserver, "handleGetTSOServiceNodeURLs");
    (void) request;

    GetTSOServiceNodeURLsResponse response{.serviceNodeURLs = _workersURLs};
    K2LOG_D(log::tsoserver, "returned TSO service nodes endpoints are: {}", _workersURLs);
    return RPCResponse(Statuses::S200_OK("OK"), std::move(response));
}

// really send the _controlInfoToSend to worker, and only place to set IsReadyToIssueTS inside _controlInfoToSend based on current state
seastar::future<> TSOService::TSOController::SendWorkersControlInfo()
{
    // step 1/3 decide IsReadyToIssueTS to be true or not
    bool readyToIssue = false;
    uint64_t curTimeTSECount = TimeAuthorityNow();

    // worker can only issue TS under following condition
    if (!_stopRequested &&                                              // stop is not requested
        _inSyncWithCluster &&                                           // our time is correct according the cluster gossip
        (_controlInfoToSend.ReservedTimeThreshold > curTimeTSECount || _controlInfoToSend.IgnoreThreshold))    // new ReservedTimeThreshold is in the future OR it is ignored
    {
        readyToIssue = true;
    }
    else
    {
        K2LOG_W(log::tsoserver, "TSO Server not ready to serve. _stopRequested:{}, _inSyncWithCluster:{}, ReservedTimeThreshold obsoleted:{},  IgnoreThreshold:{}", _stopRequested, _inSyncWithCluster, (_controlInfoToSend.ReservedTimeThreshold <= curTimeTSECount), _controlInfoToSend.IgnoreThreshold);
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

            // also update ReservedTimeThreshold after we sync time with atomicClock.
            _controlInfoToSend.ReservedTimeThreshold = GenNewReservedTimeThreshold();

            //TODO: consider update worker less frequently than timeSync
            return SendWorkersControlInfo();
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

void TSOService::TSOController::ClusterGossip()
{
    // cluster gossip task will do nothing when _stopRequested
    if (_stopRequested)
        return;

    _timeSyncFuture = DoClusterGossip().then( [this] () mutable
    {
        if (!_stopRequested)
        {
            _clusterGossipTimer.arm(_clusterGossipTimerInterval());
        }
    });

    return;
}

seastar::future<> TSOService::TSOController::DoClusterGossip()
{
    //TODO: implement HUYGEN algorithm in gossip and verify our own TimeAuthority/AtomicClock
    //      further more, even our AtomicClock may be off, it may be consistent off from cluster's time with no/little drifting
    //      consider insert an adjustment, instead of disable this server

    auto curTime = TimeAuthorityNow();

    if (_clusterGossipTimerInterval().count() < (int64_t)(curTime - _lastClusterGossipTime))
    {
        K2LOG_D(log::tsoserver, "skip DoClusterGossip, last clust gossip time:{}, gossip timer interval:{}", _lastClusterGossipTime, _clusterGossipTimerInterval().count());
        return seastar::make_ready_future<>();
    }
    else
    {
        K2LOG_D(log::tsoserver, "DoClusterGossip, last clust gossip time:{}", _lastClusterGossipTime);
        _lastClusterGossipTime = curTime;
    }

    // TODO: updating cluster members, check time etc.
    //_TSOServerURLs.push_back(k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->url);
    // K2LOG_I(log::tsoserver, "TSO Server TCP endpoints are: {}", _TSOServerURLs);
    _inSyncWithCluster = true;

    if (!_inSyncWithCluster)
    {
        K2LOG_F(log::tsoserver, "This TSO server time is off from cluster based on gossip result. It is not able to serve client request! Please check its atomic clock!");
    }

    return seastar::make_ready_future<>();

}

void TSOService::TSOController::CollectAndReportStats()
{
    _statsUpdateFuture = DoCollectAndReportStats().then( [this] () mutable
    {
        if (!_stopRequested)
        {
            _statsUpdateTimer.arm(_statsUpdateTimerInterval());
        }
    });

    return;
}

seastar::future<> TSOService::TSOController::DoCollectAndReportStats()
{
    return seastar::make_ready_future<>();
}

}
