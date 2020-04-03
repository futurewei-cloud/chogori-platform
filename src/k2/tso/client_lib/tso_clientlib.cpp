#include <random>
#include <algorithm>

#include <seastar/core/sleep.hh>

#include <k2/transport/RPCDispatcher.h>  // for RPC
#include <k2/transport/RetryStrategy.h>

#include "tso_clientlib.h"

namespace k2
{

seastar::future<> TSO_ClientLib::start()
{
    K2INFO("start");
    _stopped = false;

    // for now we use the first server URL only, in the future, allow to check other server in case first one is not available
    return seastar::sleep(_startDelay)
        .then([this] () mutable { return DiscoverServerWorkerEndPoints(_tSOServerURLs[0]); });
}

seastar::future<> TSO_ClientLib::stop()
{
    K2INFO("stop");
    if (_stopped) {
        return seastar::make_ready_future<>();
    }

    _stopped = true;

    for (auto&& clientRequest : _pendingClientRequests)
    {
        clientRequest._promise->set_exception(TSOClientLibShutdownException());
    }
    _pendingClientRequests.clear();

    //TODO: consider gracefully record outgoing batch request to TSO server and set exception to them as well.
    //currently, only in its continuation do nothing if stop is called. Should be ok except if this object is quickly deleted.


    return seastar::make_ready_future<>();
}

seastar::future<> TSO_ClientLib::DiscoverServerWorkerEndPoints(const std::string& serverURL)
{
    auto myRemote = k2::RPC().getTXEndpoint(serverURL);
    auto retryStrategy = seastar::make_lw_shared<k2::ExponentialBackoffStrategy>();
    retryStrategy->withRetries(5).withStartTimeout(1s).withRate(5);

    return retryStrategy->run([this, myRemote=std::move(myRemote)](size_t retriesLeft, k2::Duration timeout)
    {
        K2INFO("Sending with retriesLeft=" << retriesLeft << ", and timeout=" << k2::msec(timeout).count()
                    << "ms, with " << myRemote->getURL());
        if (_stopped)
        {
            K2INFO("Stopping retry since we were stopped");
            return seastar::make_exception_future<>(TSOClientLibShutdownException());
        }

        return k2::RPC().sendRequest(dto::Verbs::GET_TSO_WORKERS_URLS, myRemote->newPayload(), *myRemote, timeout)
        .then([this](std::unique_ptr<k2::Payload> payload) {
            if (_stopped) return seastar::make_ready_future<>();

            if (!payload || payload->getSize() == 0)
            {
                K2ERROR("Remote end did not provide a data endpoint. Giving up");
                return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
            }

            std::vector<std::vector<k2::String>> workerURLs;
            payload->read(workerURLs);
            K2ASSERT(!workerURLs.empty(), "TSO server should have workers");

            _curTSOServerWorkerEndPoints.clear();
            // each worker may have mulitple endPoints URLs, we only pick the fastest supported one, currently RDMA, if no RDMA, pick TCPIP
            for (auto& singleWorkerURLs : workerURLs)
            {
                k2::TXEndpoint endPointToAdd;
                for (auto& url : singleWorkerURLs)
                {
                    auto tempEndPoint = *(k2::RPC().getTXEndpoint(url));
                    K2INFO("Found remote data endpoint: " << url);
                    if (tempEndPoint.getProtocol() == RRDMARPCProtocol::proto)
                    {
                        // if found RDMA, use it and break out
                        endPointToAdd = tempEndPoint;
                        break;
                    }
                    else if (tempEndPoint.getProtocol() == TCPRPCProtocol::proto)
                    {
                        // keep it to enPointToAdd, maybe replaced by RDMA endpoint later
                        endPointToAdd = tempEndPoint;
                    }
                }
                _curTSOServerWorkerEndPoints.emplace_back(endPointToAdd);
            }

            K2ASSERT(!_curTSOServerWorkerEndPoints.empty(), "workers should property configured")

            // to reduce run-time computation, we shuffle the _curTSOServerWorkerEndPoints here
            // to simulate random pick of workers(load balance) in run time by increment a moded index
            std::random_device rd;
            std::mt19937 ranAlg(rd());

            std::shuffle(_curTSOServerWorkerEndPoints.begin(), _curTSOServerWorkerEndPoints.end(), ranAlg);

            return seastar::make_ready_future<>();
        })
        .then_wrapped([this](auto&& fut) {
            if (_stopped)
            {
                fut.ignore_ready_future();
                return seastar::make_ready_future<>();
            }
            return std::move(fut);
        });
    })
    .finally([retryStrategy]()
    {
        K2INFO("Finished getting remote data endpoint");
    });
}

seastar::future<Timestamp> TSO_ClientLib::GetTimestampFromTSO(const TimePoint& requestLocalTime)
{
    if (_stopped)
    {
        K2INFO("Stopping issuing timestamp since we were stopped");
        return seastar::make_exception_future<Timestamp>(TSOClientLibShutdownException());
    }

    // step 1/4 - sanity check if we got out of order client timestamp request
    if (requestLocalTime < _lastSeenRequestTime)
    {
        // crash in debug and error log and exception in production.
        K2ASSERT(false, "requestLocalTime " <<requestLocalTime.time_since_epoch().count() <<" is older than _lastSeenRequestTime " << _lastSeenRequestTime.time_since_epoch().count());
        K2ERROR("requestLocalTime " <<requestLocalTime.time_since_epoch().count() <<" is older than _lastSeenRequestTime " << _lastSeenRequestTime.time_since_epoch().count());
        return seastar::make_exception_future<Timestamp>(new TimeStampRequestOutOfOrderException(requestLocalTime.time_since_epoch().count(), _lastSeenRequestTime.time_since_epoch().count()));
    }
    else
    {
        _lastSeenRequestTime = requestLocalTime;
    }

    // step 2/4 - if we have timestamp from existing available batch, and they can be issued, directly get that and return
    //          note, need to remove obsolete batch(s) from begining of deque if any
    while (!_timestampBatchQue.empty())
    {
        auto& headBatch = _timestampBatchQue.front();

        // if this is available/returned batch but obsolete, remove it
        if (headBatch._isAvailable)
        {
            // we can only have available batch leftover only after we already fulfilled all the pending client request
            K2ASSERT(_pendingClientRequests.empty(), "Available timestamp batch when there is pending client request");

            // this batch must still have some timestamp
            K2ASSERT(headBatch._usedCount < headBatch._batch.TSCount, "We should not kept used-up batches.");

            // if obsolete, remove it and retry issuing timestamp from next batch at front.
            if (headBatch.ExpirationTime() < requestLocalTime)
            {
                _timestampBatchQue.pop_front();
                continue;
            }

            // we are here means that the headBatch has timestamp ready to issue
            Timestamp result = TimestampBatch::GenerateTimeStampFromBatch(headBatch._batch, headBatch._usedCount);
            headBatch._usedCount++;
            // update _lastIssuedBatchTriggeredTime
            _lastIssuedBatchTriggeredTime = _lastIssuedBatchTriggeredTime < headBatch._triggeredTime ? headBatch._triggeredTime : _lastIssuedBatchTriggeredTime;
            // remove the batch if used up.
            if (headBatch._usedCount == headBatch._batch.TSCount)
            {
                _timestampBatchQue.pop_front();
            }

            return seastar::make_ready_future<Timestamp>(result);
        }
        else
        {
            // this batch is not returned yet, can't issue timestamp immediately
            break;
        }
    }

    // if we couldn't return a ready timestamp, we need to create the request promise and return the future of it in all following difference cases.
    ClientRequest curRequest;
    curRequest._requestTime = requestLocalTime;
    curRequest._promise = seastar::make_lw_shared<seastar::promise<Timestamp>>();
    // TODO: get config from appBase and use min batch size, default 4
    uint16_t batchSizeToRequest = 4;

    // step 3/4 - there was no ready timestamp to issue. First check if there is already outgoing batch request and we can piggy back
    //        - If not, issue a new batch request and return a promise.
    if (!_timestampBatchQue.empty())
    {
        K2ASSERT(!(_timestampBatchQue.back()._isAvailable), "The last batch should still not coming back yet!");
        K2ASSERT(!(_timestampBatchQue.front()._isAvailable), "The first batch, actually every batch, should still not coming back yet!");
        auto& backBatch = _timestampBatchQue.back();
        // check if we can piggy back the last batch that is not back yet, the condition is
        // a) The last batch expected TTL include current request time
        // b) Then number of pending client requests for the last batch is smaller than the batch size
        bool canPiggyBack = (backBatch._triggeredTime.time_since_epoch().count() + backBatch._expectedTTL) > requestLocalTime.time_since_epoch().count();
        if (canPiggyBack)         // TTL is ok, now check pending count
        {
            uint16_t pendingRequestCountForBackBatch = 0;
            for(auto it = _pendingClientRequests.crbegin(); it != _pendingClientRequests.crend(); it++)
            {
                //K2ASSERT(it->_requestTime >= backBatch._triggeredTime, "Outgoing batch request must started before the client request.");

                // Quick (and dirty check), we only check the pending client request that is issued at or after last batch is issued to server
                // even those pending client requests issued before that could use the last batch
                if (it->_requestTime >= backBatch._triggeredTime
                    && pendingRequestCountForBackBatch < backBatch._expectedBatchSize)
                {
                    pendingRequestCountForBackBatch++;
                }
                else
                {
                    break;
                }
            }

            canPiggyBack = pendingRequestCountForBackBatch < backBatch._expectedBatchSize;

            // there are too many client requests already waiting for the existing batch, so we can't piggy back
            // in this case, we double the size of next batch from last one
            if (pendingRequestCountForBackBatch >= backBatch._expectedBatchSize)
            {
                // TODO: get config from appBase and use max batch size, default 32
                batchSizeToRequest = std::min(backBatch._expectedBatchSize * 2, 32);
            }
        }

        if (canPiggyBack)
        {
            curRequest._triggeredBatchRequest = false; // no op, just for readability
            _pendingClientRequests.push_back(std::move(curRequest));
            return _pendingClientRequests.back()._promise->get_future();
        }
    }

    // step 4/4 - we are here as _timestampBatchQue.empty() or we can't PiggyBack the last batch request,
    //          issue a new batch request to TSO server and return the future for the request.
    TimestampBatchInfo newBatchRequest;
    newBatchRequest._triggeredTime = requestLocalTime;  // same as curRequest._requestTime
    newBatchRequest._expectedBatchSize = batchSizeToRequest;
    newBatchRequest._expectedTTL = 8000;    // in nanosecond, TODO: use config value instead.
    _timestampBatchQue.emplace_back(std::move(newBatchRequest));

    (void) GetTimestampBatch(batchSizeToRequest)
        .then([this, triggeredTime = requestLocalTime](TimestampBatch&& newBatch) {
            ProcessReturnedBatch(std::move(newBatch), triggeredTime);
        });

    curRequest._triggeredBatchRequest = true;
    _pendingClientRequests.push_back(std::move(curRequest));
    return _pendingClientRequests.back()._promise->get_future();
}

void TSO_ClientLib::ProcessReturnedBatch(TimestampBatch batch, TimePoint batchTriggeredTime)
{
    if (_stopped)
    {
        K2INFO("Stopping process timestampbatch since we were stopped");
        return;
    }

    // step 1/4 - check if the incoming batch is obsolete one, if yes, discard it and do nothing more.
    // We check obsoleteness by meeting one of two conditions
    // a) the batchTriggeredTime < _lastIssuedBatchTriggeredTime, this means the batch coming in late and out of order, we can use it any more.
    // b) the batchTriggeredTime + TTL < the min_timepoint_bar, which coming from current time or the first pending client request's time, defined as following:
    //      the timepoint bar we use to check batch obsolete is either the first pending client timestamp request's time or
    //      if there is no pending request, use now, as any upcoming client requests' time will be bigger than now().
    if (batchTriggeredTime < _lastIssuedBatchTriggeredTime)
    {
        //TODO: log more detailed infor
        K2WARN("TimestampBatch comes in out of order and late, discarded");
        return;
    }
    TimePoint minTimePointBar = _pendingClientRequests.empty()? Clock::now() : _pendingClientRequests.front()._requestTime;
    if(batchTriggeredTime.time_since_epoch().count() + batch.TTLNanoSec < minTimePointBar.time_since_epoch().count())
    {
        //TODO: log more detailed infor
        K2WARN("TimestampBatch comes in out of order and late, discarded");
        return;
    }

    // step 2/4 Now, this batch is a keeper, match the incoming batch in the _timestampBatchQue, with removal of precedent entries that
    //  a) precedent existing available batchs, but obsolete, at the font of _timestampBatchQue
    //  b) any unavailable/outgoing batches that is triggered before this incoming batch, as this batch is coming in early, out of order.
    // NOTE: For case b), regardless if there is pending client requests, we will dicard such precedent unavailable batches. The reason is
    //       If there are pending client requests, we want fulfill them asap with this batch (and assumption is out of order batch is not likely)
    //       If there is no pending client requuest, these unavailable batches can be safely removed.
    auto ite = _timestampBatchQue.begin();
    // remove case a)
    while (ite != _timestampBatchQue.end() &&
        ite->_isAvailable &&
        ite->ExpirationTime() < minTimePointBar)
    {
         K2ASSERT(ite->_usedCount < ite->_batch.TSCount, "we should not have used-up batch still kept around!");

        _timestampBatchQue.pop_front();
        ite = _timestampBatchQue.begin();
    }
    // remove case b)
    ite = _timestampBatchQue.begin();
    while (ite != _timestampBatchQue.end() &&
        !ite->_isAvailable &&
        ite->_triggeredTime < batchTriggeredTime)
    {
        _timestampBatchQue.pop_front();
        ite = _timestampBatchQue.begin();
    }
    // now match it, if we don't find a match, this must be a bug. But we can still use it, so log error and insert it in production and crash in debug.
    K2ASSERT(ite != _timestampBatchQue.end(), "")

    if (ite == _timestampBatchQue.end() || ite->_triggeredTime > batchTriggeredTime)
    {
        // above Assert should crash in debug build, but in production, let's allow this batch
        K2WARN("A valid batch returned but its shell was unexpected removed already!");
        TimestampBatchInfo batchInfo;
        batchInfo._batch = batch;
        batchInfo._isAvailable = true;
        batchInfo._triggeredTime = batchTriggeredTime;
        batchInfo._expectedBatchSize = batch.TSCount;
        batchInfo._expectedTTL = batch.TTLNanoSec;
        _timestampBatchQue.emplace_front(std::move(batchInfo));
    }
    else
    {
        K2ASSERT(ite->_triggeredTime == batchTriggeredTime, "Find the original shell of the batch in _timestampBatchQue");
        K2ASSERT(ite->_isAvailable == false && ite->_usedCount == 0, "the batch was not available till now.")
        ite->_batch = batch;
        ite->_isAvailable = true;
    }

    // step 3/4 if any pending client request in _pendingClientRequests, start to fulfil them in order with the existing batch(es)
    uint16_t _pendingClientRequestsCount = (uint16_t) _pendingClientRequests.size();
    if (_pendingClientRequestsCount > 0)
    {
        // there are pending client request, in our design, we now can have only one available batch at the front of _timestampBatchQue,
        //as we aggressively fulfill client request when client request arrives or batch comes back, so execpt current incoming batch,
        // we can't have other available batch in _timestampBatchQue.
        K2ASSERT(_timestampBatchQue.size() == 1 || !_timestampBatchQue[1]._isAvailable, "We don't expect other available batch!");

        auto& batchInfo = _timestampBatchQue.front();
        // update _lastIssuedBatchTriggeredTime as we are about to issue from this batch
        _lastIssuedBatchTriggeredTime = _lastIssuedBatchTriggeredTime < batchInfo._triggeredTime ? batchInfo._triggeredTime : _lastIssuedBatchTriggeredTime;

        // fulfill as much pending client request as possible, while delete fulfilled pending request
        while (batchInfo._usedCount < batchInfo._batch.TSCount && !_pendingClientRequests.empty())
        {
            _pendingClientRequests.front()._promise->set_value(TimestampBatch::GenerateTimeStampFromBatch(batchInfo._batch, batchInfo._usedCount));
            _pendingClientRequests.pop_front();
            batchInfo._usedCount++;       
            _pendingClientRequestsCount--;
        }
        
        // remove this batch if it is used up
        if (_timestampBatchQue.front()._usedCount == _timestampBatchQue.front()._batch.TSCount)
        {
            _timestampBatchQue.pop_front();
        }
    }

    // step 4/4 if all available batches are used up and existing unavailable/outgoing batches is not enough to fulfill all the pending client request
    // issue replacement batch request
    if (_pendingClientRequestsCount > 0)
    {
        uint16_t expectedTSCount = 0;
        uint16_t batchSizeToRequest = 0;
        const auto& cTimestampBatchQue = _timestampBatchQue;
        for (auto&& batchInfo : cTimestampBatchQue)
        {
            K2ASSERT(!batchInfo._isAvailable, "We should not have available batch not fulfilled to client request");
            expectedTSCount += batchInfo._expectedBatchSize;
        }

        batchSizeToRequest = expectedTSCount >= _pendingClientRequestsCount ? expectedTSCount - _pendingClientRequestsCount : 0;

        if (batchSizeToRequest > 0)
        {
            // TODO: get config from appBase and use max batch size, default 32
            batchSizeToRequest = std::min(batchSizeToRequest, (uint16_t)32);

            TimePoint curTime = Clock::now();
            TimestampBatchInfo newBatchRequest;
            newBatchRequest._triggeredTime = curTime;
            newBatchRequest._expectedBatchSize = batchSizeToRequest;
            newBatchRequest._expectedTTL = 8000;    // in nanosecond, TODO: use config value instead.
            newBatchRequest._isTriggeredByReplacement = true;  // this is a replacement
            _timestampBatchQue.emplace_back(std::move(newBatchRequest));

            (void) GetTimestampBatch(batchSizeToRequest)
                .then([this, triggeredTime = curTime](TimestampBatch newBatch) {
                    ProcessReturnedBatch(newBatch, triggeredTime);
            });
        }
    }
}

seastar::future<TimestampBatch> TSO_ClientLib::GetTimestampBatch(uint16_t batchSize)
{
    auto retryStrategy = k2::ExponentialBackoffStrategy();
    //TODO: need to find out if the TSO is local or remote and get the timeout config accordingly
    retryStrategy.withRetries(3).withStartTimeout(10ms).withRate(5);

    return seastar::do_with(std::move(retryStrategy), TimestampBatch(), [this, batchSize]
        (ExponentialBackoffStrategy& rs, TimestampBatch& batch) mutable
    {
        return rs.run([this, batchSize, &batch](int retriesLeft, k2::Duration timeout)  mutable
        {
            if (_stopped)
            {
                K2INFO("Stopping retry since we were stopped");
                return seastar::make_exception_future<>(TSOClientLibShutdownException());
            }

            K2ASSERT(!_curTSOServerWorkerEndPoints.empty(), "we should have workers");
            // pick next worker (effecitvely random one, as _curTSOServerWorkerEndPoints is shuffled already when it is populated)
            int randWorker = (_curWorkerIdx++) %  _curTSOServerWorkerEndPoints.size();

            auto myRemote = _curTSOServerWorkerEndPoints[randWorker];
            std::unique_ptr<Payload> payload = myRemote.newPayload();
            payload->write(batchSize);

            (void) retriesLeft;
            // K2INFO("Requesting timestampBatch with retriesLeft=" << retriesLeft << ", and timeout=" << k2::usec(timeout).count()
            //        << "us, with worker " << randWorker);

            return k2::RPC().sendRequest(dto::Verbs::GET_TSO_TIMESTAMP_BATCH, std::move(payload), myRemote, timeout)
            .then([this, &batch](std::unique_ptr<k2::Payload> replyPayload) mutable {
                if (_stopped)
                {
                    K2INFO("Stopping retry since we were stopped");
                    return seastar::make_exception_future<>(TSOClientLibShutdownException());
                }

                if (!replyPayload || replyPayload->getSize() == 0)
                {
                    K2ERROR("TSO worker remote end did not provide a data. Giving up");
                    return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
                }

                TimestampBatch result;
                replyPayload->read(result);
                batch = std::move(result);
                return seastar::make_ready_future();
            });
        })
        .then([&batch] () mutable
        {
            return std::move(batch);
        });
    });

}

}
