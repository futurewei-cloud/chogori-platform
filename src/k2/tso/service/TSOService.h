#pragma once
#include <chrono>
#include <climits>
#include <tuple>

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include <k2/appbase/Appbase.h>
#include <k2/common/Chrono.h>
#include <k2/tso/tso_common.h>

namespace k2 
{
// TSO (controller) internal API verbs to Paxos for heart beat etc. and to Atomic/GPS clock for accurate time
enum TSOInternalVerbs : k2::Verb {
    GET_PAXOS_LEADER_URL    = 110,  // API from TSO controller to any Paxos instance to get leader instance URL
    UPDATE_PAXOS            = 111,  // API from TSO controller to Paxos leader to send heart beat(conditional write with read) and other updates(compete for master, etc)
    ACK_PAXOS               = 112,  // ACK from PAXOS to TSO 
    GET_ATOMIC_CLOCK_TIME   = 115,  // API from TSO controller to its atomic clock to get current time 
    GET_GPS_CLOCK_TIME      = 116,  // API from TSO client to get timestamp batch from any TSO worker cores
    ACK_TIME                = 117   // ACK to TSO client for above APIs
};    

// TSOService is reponsible to provide batches of K2 TimeStamps to TSO client upon request.
class TSOService 
{
public: // types

    // the control infor from Controller setting to all workers
    // all ticks are in nanoseconds
   struct TSOWorkerControlInfo
    {
        bool        IsReadyToIssueTS;       // if this core is allowed to issue TS, could be false for various reasons (TODO: consider adding reasons)
        uint8_t     TbeNanoSecStep;         // step to skip between timestamp in nanoSec, actually same as the number of worker cores 
        uint64_t    TbeTESAdjustment;       // batch ending time adjustment from current chrono::system_clock::now(), in nanoSec;
        uint16_t    TsDelta;                // batch starting time adjustment from TbeTSEAdjustment, basically the uncertainty window size, in nanoSec
        uint64_t    ReservedTimeShreshold;  // reservedTimeShreshold upper bound, the generated batch and TS in it can't be bigger than that, in nanoSec counts
        uint16_t    BatchTTL;               // TTL of batch issued in nanoseconds, not expected to change once set

        TSOWorkerControlInfo() : IsReadyToIssueTS(false), TbeNanoSecStep(0), TbeTESAdjustment(0), TsDelta(0), ReservedTimeShreshold(0), BatchTTL(0) {};
    };

    // TODO: worker/controller statistics structure typedef

public :  // application lifespan
    TSOService(k2::App& baseApp);
    ~TSOService();

    // required for seastar::distributed interface
    seastar::future<> stop();
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

    // forward delclaration of controller and worker roles
    // Each core in TSOServerice takes one role, core 0 take controller role and the rest of cores takes worker roles.
    // worker core is responsible to take requests from TSO client to issue timestamp(batch)
    // controller core is responsible to manage this process instance(participate master election, etc), 
    // periodically sync up with atomic/GPS clock and update reserved timeshreshold, etc. 
    class TSOController;
    class TSOWorker;

     // members
    k2::App& _baseApp;

    // these two roles do not exist on one core at the same time
    std::unique_ptr<TSOController> _controller;
    std::unique_ptr<TSOWorker> _worker;

};  // class TSOService

// TSOController - core 0 of a TSO server, all other cores are TSOWorkers.
// responsible to handle following four things
// 1. Upon start, join the cluster and get the for instance (role of instance can change upon API SetRole() as well)
// 2. Upon role change, set or adjust heartbeat - If master role, heartbeat also extends lease and extends ReservedTimeShreshold. If standby role, hearbeat check master's lease/healthness.
//    In master role, if ReservedTimeShreshold get extended, update TSOWorkerControlInfo to all workers.
// 3. Periodically checkAtomicGPSClock, and adjust TbeTESAdjustment if needed and further more update TSOWorkerControlInfo to all workers upon such adjustment. Note: If not master role, doing this for optimization.
// 4. If master role, periodically collect statistics from all worker cores and reports.
class TSOService::TSOController
{
    public:
    TSOController(TSOService& outer) : 
        _outer(outer), 
        _heartBeatTimer([this]{this->HeartBeat();}), 
        _timeSyncTimer([this]{this->TimeSync();}), 
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
    seastar::future<> stop();

    DISABLE_COPY_MOVE(TSOController);

    private:

    // Design Note:
    // 1. Interaction between controller and workers
    //    a) during start(), controller collect workers URLs 
    //       and if controller after JoinServerCluster() find itself is master, 
    //       it will UpdateWorkerControlInfo() through out of band DoHeartBeat() to enable workers start serving requests
    //    b) Once started, controller will only UpdateWorkerControlInfo() through regular HeartBeat()
    // 2. Internally inside controller
    //    a) during start(), initialize controller JoinServerCluster() and if master, update workers through out of band DoHeartBeat()
    //    b) Once started, only periodically HeartBeat() will handle all complex logic including role change, updating worker, handle gracefully stop()/lost lease suicide().
    //    c) TimeSyn() only update in memory _controlInfoToSend, which will be sent with next HeartBeat(); 

    // First step of Initialize controller before JoinServerCluster() during start() 
    seastar::future<> InitializeInternal();

    // initialize TSOWorkerControlInfo at start()
    inline void InitWorkerControlInfo();

    seastar::future<> GetAllWorkerURLs();

    // Join the TSO server cluster during start().
    // return tuple 
    //  - element 0 - if this instance is a master or not.
    //  - element 1 - prevReservedTimeShreshold if this instance is mater, the value need to be waited out by this master instnace to avoid duplicate timestamp.
    // TODO: implement this
    seastar::future<std::tuple<bool, uint64_t>> JoinServerCluster() 
    {
        K2INFO("JoinServerCluster");
        // fake new master
        std::tuple<bool, uint64_t> result(true, 0);
        _myLease = GenNewLeaseVal();
        _masterInstanceURL = k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->getURL();
        return seastar::make_ready_future<std::tuple<bool, uint64_t>>(result);
    }

    // APIs registration
    // APIs to TSO clients
    void RegisterGetTSOMasterURL();
    void RegisterGetTSOWorkersURLs();
    // internal API responses Paxos and Atomic/GPS clock();
    void RegisterACKPaxos() { return; }
    void RegisterACKTime() { return; }

    // TODO: implement this
    seastar::future<> ExitServerCluster() 
    {
        return seastar::make_ready_future<>();
    }

    // Change my role inside the controller, from Master to StandBy(isMaster passed in is true) or from Standby to Master
    // SetRoleInternal will, if needed, trigger out of band heartbeat and worker control info update, to prepare workers in ready mode
    // Note: Asssumption - When this is called, Paxos already had properly updated master related record. This assumption is also the reason this fn is called "Internal" (of the controller object)   
    //       This fn is called during start() after JoinServerCluster, and during regular HeartBeat() which could find out role change.
    seastar::future<> SetRoleInternal(bool isMaster, uint64_t prevReservedTimeShreshold);

    // periodically send heart beat and handle heartbeat response
    // If this is Master Instance, heart beat will renew the lease, extend the ReservedTimeShreshold if needed
    // If this is Standby Instance, heart beat will maintain the membership, and check Master Instance status and take master role if needed
    void HeartBeat();

    // helper to do the HeartBeat(), could be called from regular HeartBeat(), or during initialization or inside HearBeat() when role need to be changed
    seastar::future<> DoHeartBeat();

    // helper for DoHeartBeat() when _stopRequested
    seastar::future<> DoHeartBeatDuringStop();

    // this is lambda set in HeartBeat() to handle the response,
    // For standby instance, may 
    seastar::future<> HandleHeartBeatResponse() { return seastar::make_ready_future<>(); }

    // TimeSync timer call back fn.
    void TimeSync();
    // helper function which do the real work of time sync.
    seastar::future<> DoTimeSync();

    // check atomic/GPS clock and return an uncertainty windows of time containing current real time
    seastar::future<std::tuple<SysTimePt, SysTimePt>> CheckAtomicGPSClock();

    // Once we have updated controlInfo due to any reason, e.g. role change, ReservedTimeShreshold or drift from atomic clock, 
    // propagate the update to all workers and
    // The control into to send is at member _controlInfoToSend, except IsReadyToIssueTS, which will be set inside this fn based on the current state
    seastar::future<> SendWorkersControlInfo();

    // periodically collect stats from workers and report
    void CollectAndReportStats();
    seastar::future<> DoCollectAndReportStats();


    // suicide when and only when we are master and find we lost lease
    void Suicide();


    // helpers to talk to Paxos
    // TODO: Consider role change, 
    // TODO: implement this
    seastar::future<> RemoveLeaseFromPaxos() {return seastar::make_ready_future<>();}
    seastar::future<> RemoveLeaseFromPaxosWithUpdatingReservedTimeShreshold(/*uint64_t newReservedTimsShreshold*/) {return seastar::make_ready_future<>();}
    seastar::future<SysTimePt> RenewLeaseOnly() {return seastar::make_ready_future<SysTimePt>(GenNewLeaseVal());}

    // regular heartbeat update to Paxos when not a master
    seastar::future<> UpdateStandByHeartBeat() {return seastar::make_ready_future<>();}

    // regular heartbeat update to Paxos when is a master
    // return future contains newly extended Lease and ReservedTimeThreshold
    seastar::future<std::tuple<SysTimePt, SysTimePt>>RenewLeaseAndExtendReservedTimeThreshold()
    {
    
        auto extendedLeaseAndThreshold = GenNewLeaseVal(); 
        std::tuple<SysTimePt, SysTimePt> tup(extendedLeaseAndThreshold, extendedLeaseAndThreshold);
        return seastar::make_ready_future<std::tuple<SysTimePt, SysTimePt>>(tup);
    }

    // three times of heartBeat + 1 extra millisecond to allow missing up to 3 heartbeat before loose leases
    inline SysTimePt GenNewLeaseVal() { return SysClock::now() + _heartBeatTimerInterval() * 3 + 1ms;}

    // outer TSOService object
    TSOService& _outer;

    // _isMasterInstance, set when join cluster or with heartbeat
    bool _isMasterInstance{false};

    // URL of current TSO master instance
    k2::String _masterInstanceURL;

    // worker cores' URLs, each worker can have mulitple urls
    std::vector<std::vector<k2::String>> _workersURLs;
    
    // when this instance become (new) master, it need to get previous master's ReservedTimeShreshold 
    // and wait out this time if current time is less than this value
    uint64_t _prevReservedTimeShreshold{ULLONG_MAX};

    // Lease at the Paxos, whem this is master, updated by heartbeat.
    SysTimePt _myLease{0ms};

    // set when stop() is called 
    bool _stopRequested{false};

    // last sent (to workers) worker control info
    TSOWorkerControlInfo _lastSentControlInfo;
    // current control info that is updated and to be sent to worker
    // Note: IsReadyToIssueTS is only set inside SendWorkersControlInfo() based on the state when SendWorkersControlInfo() is called
    TSOWorkerControlInfo _controlInfoToSend;  

    seastar::timer<> _heartBeatTimer;
    ConfigDuration _heartBeatTimerInterval{"tso.ctrol_heart_beat_interval", 10ms};
    seastar::future<> _heartBeatFuture = seastar::make_ready_future<>();  // need to keep track of heartbeat task future for proper shutdown 

    seastar::timer<> _timeSyncTimer;
    ConfigDuration _timeSyncTimerInterval{"tso.ctrol_time_sync_interval", 10ms};
    seastar::future<> _timeSyncFuture = seastar::make_ready_future<>();  // need to keep track of timeSync task future for proper shutdown 
    ConfigDuration _batchTTL{"tso.ctrol_ts_batch_TTL", 8ms};
    ConfigDuration _defaultTSBatchEndAdj{"tso.ctrol_ts_batch_end-adj", 8ms}; // this is also the batch uncertainty windows size

    seastar::timer<> _statsUpdateTimer;
    ConfigDuration _statsUpdateTimerInterval{"tso.ctrol_stats_update_interval", 1s};
    seastar::future<> _statsUpdateFuture = seastar::make_ready_future<>();  // need to keep track of statsUpdate task future for proper shutdown 

};

// TSOWorker - worker cores of TSO service that take TSO client requests and issue TimeStamp (batch).
// responsible to handle following three things, if this TSO is master instance role.
// 1. handle TSO client request, issuing time stamp (batch). This is a normal priority task.
// 2. handle config data(TSOWorkerControlInfo below) update task issued from the control core. This is a high priority task.
// 3. collect and aggregate statistics data of this core for control core to collect. This is a low priority task.
class TSOService::TSOWorker
{
    public:
    TSOWorker(TSOService& outer) : _outer(outer){};

    seastar::future<> stop();
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

    // last request time rounded at microsecond level and count of timestamp issued in the microsecond
    // Note: each worker core can issue up to (1000/TbeNanoSecStep) timestamps within same microsecond
    uint64_t _lastRequestTimeMicrSecRounded{0};  // typically generated from SysTimePt_MicroSecRounded(Now())
    uint16_t _lastRequestTimeStampCount{0};

    // TODO: statistics structure

    // APIs to TSO clients
    void RegisterGetTSOTimeStampBatch();

    // the main API for TSO client to get timestamp in batch
    // batchSizeRequested may be partically fulfilled based on server side timestamp availability
    TimeStampBatch GetTimeStampFromTSO(uint16_t batchSizeRequested);
    // helper function to issue timeStamp (or check error situation) 
    TimeStampBatch GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t nowMicroSecRounded);

    // private helper
    // helpers for updateWorkerControlInfo
    void AdjustWorker(const TSOWorkerControlInfo& controlInfo);


};

} // namespace k2
