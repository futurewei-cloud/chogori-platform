#pragma once
#include <chrono>

// third-party
#include <seastar/core/distributed.hh>  // for distributed<>
#include <seastar/core/future.hh>       // for future stuff

#include <k2/appbase/Appbase.h>
#include <k2/tso/tso_common.h>

namespace k2 {



// TSOService is reponsible to provide batches of K2 TimeStamps to TSO client upon request.
class TSOService {
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
    uint32_t TSOId() {return 0;};

    seastar::future<> msgReceiver();

    // worker public APIs
    // worker API updating the controlInfo, triggered from controller through SS cross-core communication
    seastar::future<> UpdateWorkerControlInfo(const TSOWorkerControlInfo& controlInfo);

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


class TSOService::TSOController
{
    public:
    TSOController(TSOService& outer) : _outer(outer){};

    //TODO
    seastar::future<> stop() {return seastar::make_ready_future<>();};
    seastar::future<> start() {return seastar::make_ready_future<>();};

    // someone ask to change my role out of band, from Master to StandBy or from Standby to Master
    seastar::future<bool> SetRole() {return seastar::make_ready_future<bool>(true);};

    DISABLE_COPY_MOVE(TSOController);

    private:

    // join the TSO server cluster. TODO: implement this
    bool JoinServerCluster(bool* pIsMasterInstance) {*pIsMasterInstance = true; return true; }

    // periodically send heart beat and handle heartbeat response
    // If this is Master Instance, heart beat will renew the lease, extend the ReservedTimeShreshold if needed
    // If this is Standby Instance, heart beat will maintain the membership, and check Master Instance status and take master role if needed 
    // TODO: implement this
    seastar::future<bool> HeartBeat() { return seastar::make_ready_future<bool>(true); }
    // this is lambda set in HeartBeat() to handle the response, 
    // For standby instance, may 
    seastar::future<bool> HandleHeartBeatResponse() { return seastar::make_ready_future<bool>(true); }

    // Proactively gracefully release Master role due to various reasons
    // Once this is called, locally this instance will stop issuing TS and set itself as StandBy
    seastar::future<> ReleaseMasterRole() { return seastar::make_ready_future<>(); }

    // Periodically check atomic/GPS clock
    seastar::future<bool> CheckAtomicGPSClock() { return seastar::make_ready_future<bool>(true); }
    // this is lambda set in HeartBeat() to handle the response, 
    // For standby instance, may 
    seastar::future<bool> HandleAtomicGPSClockResponse() { return seastar::make_ready_future<bool>(true); }

    // Once we have updated controlInfo due to any reason, e.g. role change, ReservedTimeShreshold or drift from atomic clock, 
    // propagate the update to all workers and  
    seastar::future<bool> UpdateWorkersControlInfo(const TSOWorkerControlInfo& controInfo);

    // outer TSOService object
    TSOService& _outer;

    bool _isMasterInstance;

    // current worker control info
    TSOWorkerControlInfo _curControlInfo;

};

class TSOService::TSOWorker
{
    public:
    TSOWorker(TSOService& outer) : _outer(outer){};
    seastar::future<> stop() {return seastar::make_ready_future<>();};
    seastar::future<> start() {_tsoId = _outer.TSOId(); return seastar::make_ready_future<>();};

    DISABLE_COPY_MOVE(TSOWorker);

    // the main API for TSO client to get timestamp in batch
    // batchSizeRequested may be partically fulfilled based on server side timestamp availability
    seastar::future<TimeStampBatch> GetTimeStampFromTSO(uint16_t batchSizeRequested);

    // get updated controlInfo from controller and update local copy
    seastar::future<> UpdateWorkerControlInfo(const TSOWorkerControlInfo& controlInfo);

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

    // private helper
    // helpers for updateWorkerControlInfo
    seastar::future<> AdjustWorker(const TSOWorkerControlInfo& controlInfo);
    seastar::future<> StartWorker(const TSOWorkerControlInfo& controlInfo);
    seastar::future<> StopWorker(const TSOWorkerControlInfo& controlInfo);

    // helper function to issue timeStamp (or check error situation) 
    seastar::future<TimeStampBatch> GetTimeStampFromTSOLessFrequentHelper(uint16_t batchSizeRequested, uint64_t nowMicroSecRounded);
};

} // namespace k2
