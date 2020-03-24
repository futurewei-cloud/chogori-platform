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
// TSO client lib - providing K2 TimeStamp to app
class TSO_ClientLib
{
public:
    // constructor
    // TODO: instead of pass in TSOServerURL, we need to change later to CPO URL and get URLs of TSO servers from there instead.
    TSO_ClientLib(const std::string& tSOServerURL) { _tSOServerURLs.emplace_back(tSOServerURL); K2INFO("ctor");}
    
    ~TSO_ClientLib() { K2INFO("dtor");}

    seastar::future<> start();
    seastar::future<> stop();

    // get the timestamp from TSO (distributed from TSOClient Timestamp batch)
    seastar::future<TimeStamp> GetTimeStampFromTSO(const TimePoint& requestLocalTime);
    // get the timestamp with MTL(Minimum Transaction Latency) - alternatively instead of this new API, consider put MTL inside timestamp. 
    // seastar::future<std::tuple<TimeStamp, Duration>> GetTimeStampWithMTLFromTSO(const TimePoint& requestLocalTime);

private:

    // discover TSO server worker cores, populating _curTSOServerWorkerEndPoints, during start() and server change.
    seastar::future<> DiscoverServerWorkerEndPoints(const std::string& serverURL);

    seastar::future<TimeStampBatch> GetTimeStampBatch(uint16_t batchSize);

    // process returned batch from TSO server
    void ProcessReturnedBatch(TimeStampBatch batch, TimePoint batchTriggeredTime);

    bool _stopped{false};

    // a vector of TSO servers
    // TODO: currently just use one, we will use multiple later, with more info like location(local or remote), availability status etc. Also get them from CPO instead.
    //       the CPO should give the list of TSO servers in preference order in the vector.
    std::vector<std::string> _tSOServerURLs;

    // all URLs of workers of current TSO server
    std::vector<k2::TXEndpoint> _curTSOServerWorkerEndPoints;
    size_t _curWorkerIdx{0};

    // For debugging and verification purpose, as we are processing request with steady clock, use this to verify  
    // the requet we see are always coming in with bigger value steady clock.
    TimePoint _lastSeenRequestTime;

    // For correctness verification purpose, we keep track of the latest _triggeredTime of the batches whenever we issued timestamp from a (new) batch
    // So that if an out-of-order old batch comes in, we will discard it.  
    TimePoint _lastIssuedBatchTriggeredTime;

    // info about queued request that is promised but not yet fulfilled
    struct ClientRequest
    {
        TimePoint   _requestTime;
        seastar::lw_shared_ptr<seastar::promise<TimeStamp>> _promise;       // promise for this client request
        bool        _triggeredBatchRequest{false};  // if this client request tirggered a batch request to TSO server
    };



    // returned available timestamp batch 
    struct TimeStampBatchInfo
    {
        TimeStampBatch _batch;
        bool _isAvailable{false};   // if this issued batch is already fulfilled.                  
        uint8_t _usedCount{0};
        TimePoint _triggeredTime; // triggered time for this batch, any other later client request comes in before this value + batch TTL could be fulfilled by this batch timewise. 
        uint16_t    _expectedBatchSize{0}; // the count of timeStamp in triggered/not returned batch request, used for estimate. The TSO server may return less amount of TS
        uint16_t    _expectedTTL{0};       // in nanosecond, estimated TTL in triggered/not returned batch request. The TSO server control the value, returned in _batch.
        bool _isTriggeredByReplacement{false};   // when timestamp batch request was triggerred by replacment for the TSBatch that is returned out of order and discarded

        const TimePoint ExpirationTime()
        {
            K2ASSERT(_isAvailable, "Doesn't support ExpirationTime on unavailable TimeStampBatch as true TTL from server is not available.");

            std::chrono::nanoseconds TTL(_batch.TTLNanoSec);

            return _triggeredTime + TTL;
        }

        const TimePoint ExpectedExpirationTime()
        {
            std::chrono::nanoseconds TTL(_expectedTTL);

            return _triggeredTime + TTL;
        }
    };

    // Design Notes on matching incoming client request and outgoing batch request to TSO server
    // 1. Client side issues request to get timestamp one by one, but TSOClientLib as proxy and get timestamp batch from TSO server. 
    //    Sometime there are pending client requests waiting for batch result to fulfill, sometimes there are left over TimeStamp from returned batch(s).
    //    Thus, we have two deques,  _pendingClientRequest and _timeStampBatchQueue to hold the info. 
    // 2. Client request comes in with request time(steady clock) in order and will be only fulfilled in order as well.
    // 3. timeStamp batch coming back from TSO server(s) could be out of order occasionly, we will discard the older batch if we already start to issue timestam from newer batch
    //    When such discard happens, we may need to issue another replacment batch request to TSO server. 
    //    Also, there is case the TSO server may return a batch with less amount of timestamps that we requested, 
    //    in this case, we will issue a Replacement batch request as well with current time as triggerred time.  
    // 4. TimeStampBatch has TTL, if the client side request fits in the TTL, the request can be fulfilled with TimeStamp from the batch. 
    //    Obey the TTL is critical to guarantee (external) causal consistency in 3SI protocol. Detailed analysis is available in TSO design spec.
    // 5. When a client request comes in, if there is no other pending client request and no batch available, 
    //    a batch request will be issued to TSO server asynchonously with its placeholder entry inserted into _timeStampBatchQue and ClientRequest for this request is added into _pendingClientRequest 
    //    and the future of ClientRequest._promise is returned to the client, which will be fulfilled later when the batch returned. 
    // 6. when a client request comes in, if there is previous pending client request and no batch available,
    //    we need to check if this client request could be fulfilled with latest outgoing batch request, there are two conditions for this
    //          a) Time - if this client request time fits in batch TTL + the time of the last pending client request, 
    //          b) Count - total pending requests matched to this batch is less than the expected expetedBatchSize.
    //    if this client request could not be fulfilled with existing pending batch request, a new batch request to TSO server need to be issued. 
    // 7. When a batch returned from TSO server, we will first check if we should dicard the batch to make sure we can use it. We will discard these out of order batch in two cases
    //          a) its _triggeredTime is smaller(older) than the batch we already issued timstamp from.
    //          b) Its _triggeredTime + TTL is smaller (order) than minimal timepoint bar, which is either current time or the request time of the first pending client request.
    //    If it is not discarded, we will  into the _timeStampBatchQue matching its _triggeredTime(normally should be head if not out of order).
    //    Then, if there is any entry in _pendingClientRequest, we will try to fufill the client request. The logic is following
    //          a) remove all obsolete head entries from _timeStampBatchAvailable, i.e. those has _timeStampBatchAvailable + TTL that is less than _pendingClientRequest's head's request time
    //          b) for all available/ready enties in _timeStampBatchQue, we fulfill the pending request in time order with TTL varification. If during the process, 
    //            an unavailable batch encountered(with a newer available batch already arrived), the unavailable batch entry will be discarded and replacment batch 
    //            request will be issued, as we want to aggressively fulfil the client request as quickly as possible. 
    //            (NOTE: maybe wait a limited amount of time if two batch triggered time are very close, for optimization. So far feels no need due to cost of wait 
    //             and low chance of such out of order issue. We should evalue this again with real life cases)
    // 8. When a client request comes in, if there is batches available in _timeStampBatchQue, try to issue timestamp from availalbe batch. If these batches are obsolete,
    //    discard them from _timeStampBatchQue and issue new batch request asynchronously. 

    std::deque<ClientRequest>  _pendingClientRequests;
    std::deque<TimeStampBatchInfo> _timeStampBatchQue;
};

class TimeStampRequestOutOfOrderException : public std::exception {
    public:
    TimeStampRequestOutOfOrderException(uint64_t requestTime, uint64_t lastSeenRequestTime) 
        : _requestTime(requestTime), _lastSeenRequestTime(lastSeenRequestTime) {};

    private:
    virtual const char* what() const noexcept override { return "requestLocalTime is older than _lastSeenRequestTime "; }

    uint64_t _requestTime;
    uint64_t _lastSeenRequestTime;
};

}