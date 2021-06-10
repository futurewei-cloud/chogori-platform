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
#include <climits>
#include <tuple>

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include <k2/appbase/Appbase.h>
#include <k2/common/Chrono.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/dto/TimestampBatch.h>
#include "Log.h"

namespace k2
{

using namespace dto;

// TSOService is responsible to provide batches of K2 TimeStamps to TSO client upon request.
class TSOService
{
public: // types

    // the control info from Controller setting to all workers
    // all ticks are in nanoseconds
   struct TSOWorkerControlInfo
    {
        bool        IsReadyToIssueTS;       // if this worker core is allowed to issue TS, could be false for various reasons (TODO: consider adding reasons)
        bool        IgnoreThreshold;        // Ignore ReservedTimeThreshold to allow issue Timestamp regardless. This is for testing or single machine dev env where shreshold update can be delayed.
        uint8_t     TBENanoSecStep;         // step to skip between timestamp in nanoSec, actually same as the number of worker cores
        uint64_t    TBEAdjustment;          // batch ending time adjustment from current chrono::system_clock::now(), in nanoSec;
        uint16_t    TsDelta;                // batch starting time adjustment from TbeTSEAdjustment, basically the uncertainty window size, in nanoSec
        uint64_t    ReservedTimeThreshold;  // reservedTimeShreshold upper bound, the generated batch and TS in it can't be bigger than that, in nanoSec counts. It is extended with each TimeSync with TimeAuthority/Atomic clock.
        uint16_t    BatchTTL;               // TTL of batch issued in nanoseconds, not expected to change once set

        TSOWorkerControlInfo() : IsReadyToIssueTS(false), IgnoreThreshold(false), TBENanoSecStep(0), TBEAdjustment(0), TsDelta(0), ReservedTimeThreshold(0), BatchTTL(0) {};
    };

    // TODO: worker/controller statistics structure typedef

public :  // application lifespan
    TSOService();
    ~TSOService();

    // required for seastar::distributed interface
    seastar::future<> gracefulStop();
    seastar::future<> start();

    //TODO: implement this
    uint32_t TSOId() {return 1;};

    // worker public APIs
    // worker API updating the controlInfo, triggered from controller through SS cross-core communication
    void UpdateWorkerControlInfo(const TSOWorkerControlInfo& controlInfo);

    // get worker endpoint URLs of all transport stack, TCP/IP, RDMA, etc.
    std::vector<k2::String> GetWorkerURLs();

    // controller public APIs

private:
    // types

    // forward declaration of controller and worker roles
    // Each core in TSOServerice takes one role, core 0 take controller role and the rest of cores takes worker roles.
    // worker core is responsible to take requests from TSO client to issue timestamp(batch)
    // controller core is responsible to manage this process instance(participate master election, etc),
    // periodically sync up with atomic/GPS clock and update reserved timeshreshold, etc.
    class TSOController;
    class TSOWorker;

    // these two roles do not exist on one core at the same time
    std::unique_ptr<TSOController> _controller;
    std::unique_ptr<TSOWorker> _worker;

};  // class TSOService

// TSOController - core 0 of a TSO server, all other cores are TSOWorkers.
// responsible to handle following four things
// 1. Upon start, join the cluster and get the for instance (role of instance can change upon API SetRole() as well)
// 2. Upon role change, set or adjust heartbeat - If master role, heartbeat also extends lease and extends ReservedTimeThreshold. If standby role, hearbeat check master's lease/healthness.
//    In master role, if ReservedTimeThreshold get extended, update TSOWorkerControlInfo to all workers.
// 3. Periodically checkAtomicGPSClock, and adjust TBEAdjustment if needed and further more update TSOWorkerControlInfo to all workers upon such adjustment. Note: If not master role, doing this for optimization.
// 4. If master role, periodically collect statistics from all worker cores and reports.
class TSOService::TSOController
{
    public:
    TSOController(TSOService& outer) :
        _outer(outer),
        _timeSyncTimer([this]{this->TimeSync();}),
        _clusterGossipTimer([this]{this->ClusterGossip();}),
        _statsUpdateTimer([this]{this->CollectAndReportStats();}){};

    // start the controller
    // Assumption: caller will wait the start() fully complete
    // Internally, it will
    // 1) InitializeInternal, including init control info, gather worker URLs, sync time with atomic clock;
    // 2) then join the cluster;
    // 3) then set role (master or standby)
    // 4) then arm timers and register public RPC APIs
    seastar::future<> start();

    // stop the controller
    // Internally, it will
    // 1) set stop requested(maybe already done)
    // 2) then unregister public RPC APIs
    // 3) then wait for all three timered task done and cancel timers respectively
    // 4) then exit cluster
    // NOTE: stop may need one full cycle of heartbeat() to finish, default 10ms.
    seastar::future<> gracefulStop();

    DISABLE_COPY_MOVE(TSOController);

    private:

    // Design Note:
    //    a) during start(), controller collect workers URLs and issue first time DoTimeSync() and update in memory _controlInfoToSend
    //       and if controller after JoinServerCluster() find if out not itself could continue as the cluster (other servers) may find this server time is off and disallow it to contine
    //    b) Once can continue starting, controller will do periodically TimeSync() which will update in memory _controlInfoToSend and send to worker by calling UpdateWorkerControlInfo()

    // First step of Initialize controller before JoinServerCluster() during start()
    // Most important thing is to do a time check with local TimeAuthority/AtomicClock
    seastar::future<> InitializeInternal();

    // initialize TSOWorkerControlInfo at start()
    inline void InitWorkerControlInfo();

    seastar::future<> GetAllWorkerURLs();

    // Join the TSO server cluster during start().
    // Get other server URLs and check if this server time is in sync with that of cluster
    // TODO: implement this
    seastar::future<> JoinServerCluster()
    {
        K2LOG_I(log::tsoserver, "JoinServerCluster");
        // fake join cluster
        // currently just put myself into the cluster.
        _TSOServerURLs.push_back(k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->url);
        K2LOG_I(log::tsoserver, "TSO Server TCP endpoints are: {}", _TSOServerURLs);
        _inSyncWithCluster = true;
        return seastar::make_ready_future<>();
    }

    // APIs registration
    // APIs to TSO clients
    seastar::future<std::tuple<Status, dto::GetTSOServerURLsResponse>>
    handleGetTSOServerURLs(dto::GetTSOServerURLsRequest&& request);

    seastar::future<std::tuple<Status, dto::GetTSOServiceNodeURLsResponse>>
    handleGetTSOServiceNodeURLs(dto::GetTSOServiceNodeURLsRequest&& request);

    // TimeSync timer call back fn.
    void TimeSync();
    // helper function which do the real work of time sync.
    seastar::future<> DoTimeSync();

    // check atomic/GPS clock and return an effective uncertainty windows of time containing current real time
    // return value is actually two unint64 values<T, V>, the first one is the difference of TAI TSE(in nanosec) since Jan. 1, 1970 to local steady_clock, the second value is uncertainty window size(in nanosec)
    // The current time (uncertainty window) will be <steady_clock::now() + T - V/2, steady_clock::now() + T + V/2>
    seastar::future<std::tuple<uint64_t, uint64_t>> CheckAtomicGPSClock();

    // Once we have updated controlInfo due to any reason, e.g. role change, ReservedTimeThreshold or drift from atomic clock,
    // propagate the update to all workers and
    // The control into to send is at member _controlInfoToSend, except IsReadyToIssueTS, which will be set inside this fn based on the current state
    seastar::future<> SendWorkersControlInfo();

    // cluster gossip timer callback fn.
    void ClusterGossip();
    //  helper function which do the real work of cluster gossip
    seastar::future<> DoClusterGossip();

    // periodically collect stats from workers and report
    void CollectAndReportStats();
    seastar::future<> DoCollectAndReportStats();

    // (in nanosec counts) Current TA time + drifting allowance (in term of multiple, default 10, times of timeSync interval).
    inline uint64_t GenNewReservedTimeThreshold() {return TimeAuthorityNow() + _timeSyncTimerInterval().count() * _timeDriftingAllowanceMulitple();};

    // outer TSOService object
    TSOService& _outer;

    // if this server is in sync with others in the cluster, if not, this server can't issue timestamp
    bool _inSyncWithCluster{false};

    // tcp URLs of all current live TSO server instances in the TSO server cluster
    std::vector<k2::String> _TSOServerURLs;

    // worker cores' URLs, each worker can have multiple urls
    std::vector<std::vector<k2::String>> _workersURLs;

    // The difference between the TA(Time Authority) and local time (local steady clock as it is strictly increasing).
    // This is part of TBEAdjustment. This is kept to detect local steady_clock drift away from Time Authority at each TimeSyncTask.
    uint64_t _diffTALocalInNanosec {0};

    // known current time of TA(TimeAuthority), local steady_clock time now + the diff between, in units of nanosec since Jan. 1, 1970 (TAI)
    inline uint64_t TimeAuthorityNow() {return now_nsec_count() +  _diffTALocalInNanosec; }

    // _ignoreReservedTimeThreshold, let TSO controller and worker ignore the _ignoreReservedTimeThreshold
    // This is need for testing and single box dev env, where the controller core can be too busy to update ReservedTimeThreshold
    // as controller core(core 0) can run other process/threads, instead of being dedicated only the controller as designed in production env.
    // TODO: change the default value to false.
    ConfigVar<bool> _ignoreReservedTimeThreshold{"tso.ignore_reserved_time_threshold", true};

    // set when stop() is called
    bool _stopRequested{false};

    // last sent (to workers) worker control info
    TSOWorkerControlInfo _lastSentControlInfo;
    // current control info that is updated and to be sent to worker
    // Note: IsReadyToIssueTS is only set inside SendWorkersControlInfo() based on the state when SendWorkersControlInfo() is called
    TSOWorkerControlInfo _controlInfoToSend;

    seastar::timer<> _timeSyncTimer;
    ConfigDuration _timeSyncTimerInterval{"tso.ctrol_time_sync_interval", 10ms};
    seastar::future<> _timeSyncFuture = seastar::make_ready_future<>();  // need to keep track of timeSync task future for proper shutdown

    // local crystal clock drifting allowance, in terms of multiple of _timeSyncTimerInterval
    // Picking 10 as the local crystal clock drifting allowance is at less than 10 ms per second level, i.e. new threshold is less than 1 second away in the future is ok.
    ConfigVar<uint32_t> _timeDriftingAllowanceMulitple{"tso.control_time_drifting_allowance_multiple", 10u};

    // timer for cluster gossip
    // TODO: implement HUYGENS algorithm during gossip to further reduce uncertainty window.
    // 1000ms should be good default value as 2s is used in [HUYGENS] algorithm (https://www.usenix.org/system/files/conference/nsdi18/nsdi18-geng.pdf)
    seastar::timer<> _clusterGossipTimer;
    ConfigDuration _clusterGossipTimerInterval{"tso.ctrol_cluster_gossip_interval", 1000ms};
    seastar::future<> _clusterGossipFuture = seastar::make_ready_future<>();  // need to keep track of cluster gossip task future for proper shutdown
    uint64_t _lastClusterGossipTime{0};     // last time cluster gossip happened. After fully implementation, it could be updated during handling of incoming gossip as well as outgoing message.

    // this is the batch uncertainty windows size, should be less than MTL(minimal transaction latency),
    // this is also used at the TSO client side as batch's TTL(Time To Live)
    // TODO: consider derive this value from MTL configuration.
    // NB: this is placed into an uint16t so max value is 65usec
    ConfigDuration _defaultTBWindowSize{"tso.ctrol_ts_batch_win_size", 40us};

    seastar::timer<> _statsUpdateTimer;
    ConfigDuration _statsUpdateTimerInterval{"tso.ctrol_stats_update_interval", 1s};
    seastar::future<> _statsUpdateFuture = seastar::make_ready_future<>();  // need to keep track of statsUpdate task future for proper shutdown

};

// TSOWorker - worker cores of TSO service that take TSO client requests and issue Timestamp (batch).
// responsible to handle following three things, if this TSO is master instance role.
// 1. handle TSO client request, issuing time stamp (batch). This is a normal priority task.
// 2. handle config data(TSOWorkerControlInfo below) update task issued from the control core. This is a high priority task.
// 3. collect and aggregate statistics data of this core for control core to collect. This is a low priority task.
class TSOService::TSOWorker
{
    public:
    TSOWorker(TSOService& outer) : _outer(outer){};

    seastar::future<> gracefulStop();
    seastar::future<> start();

    DISABLE_COPY_MOVE(TSOWorker);

    // get updated controlInfo from controller and update local copy
    void UpdateWorkerControlInfo(const TSOWorkerControlInfo& controlInfo);

    // periodical task to send statistics to controller core
    seastar::future<> SendWorkderStatistics() {return seastar::make_ready_future<>();};

    private:
    // outer TSOService object
    TSOService& _outer;
    uint32_t _tsoId;    // keep a copy to avoid access _outer in TS issuing hot path

    // current worker control info
    TSOWorkerControlInfo _curControlInfo;

    // last request's TBE(Timestamp Batch End) time rounded at microsecond level
    uint64_t _lastRequestTBEMicroSecRounded{0};
    // count of timestamp issued in last request's timestamp batch
    // Note: each worker core can issue up to (1000/TBENanoSecStep) timestamps within same microsecond (at TBE)
    uint16_t _lastRequestTimeStampCount{0};

    // TODO: statistics structure

    // APIs to TSO clients
    seastar::future<std::tuple<Status, dto::GetTimeStampBatchResponse>>
    handleGetTSOTimestampBatch(dto::GetTimeStampBatchRequest&& request);

    // the main API for TSO client to get timestamp in batch
    // batchSizeRequested may be partically fulfilled based on server side timestamp availability
    TimestampBatch GetTimestampFromTSO(uint16_t batchSizeRequested);
    // helper function to issue timestamp (or check error situation)
    TimestampBatch GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t nowMicroSecRounded);

    // private helper
    // helpers for updateWorkerControlInfo
    void AdjustWorker(const TSOWorkerControlInfo& controlInfo);


};

// TSO service should be started with at least two cores, one controller and rest are workers.
class TSONotEnoughCoreException : public std::exception {
public:
    TSONotEnoughCoreException(uint16_t coreCount) : _coreCount(coreCount) {};

    virtual const char* what() const noexcept override { return "TSONotEnoughCoreException: Need at least two cores. core counts provided:" + _coreCount; }

private:
    uint16_t _coreCount {0};
};

// TSO server not ready yet to issue timestamp(batch)
// TODO: add more detail error info.
class TSONotReadyException : public std::exception {
public:
    virtual const char* what() const noexcept override { return "Server not ready to issue timestamp, please retry later."; }
};

// operations invalid during server shutdown
class TSOShutdownException : public std::exception {
public:
    virtual const char* what() const noexcept override { return "TSO Server shuts down."; }
};

} // namespace k2
