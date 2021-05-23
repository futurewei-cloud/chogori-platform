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
    // TODO: instead of using config value TSOServerURL, we need to change later to CPO URL and get URLs of TSO servers from there instead.
    K2LOG_I(log::tsoclient, "start with server url: {}", TSOServerURL());
    _stopped = false;

    _tSOServerURLs.emplace_back(TSOServerURL());
    // for now we use the first server URL only, in the future, allow to check other server in case first one is not available
    return _discoverServiceNodes(_tSOServerURLs[0]);
}

seastar::future<> TSO_ClientLib::gracefulStop() {
    K2LOG_I(log::tsoclient, "stop");
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

seastar::future<> TSO_ClientLib::_discoverServiceNodes(const k2::String& serverURL)
{
    auto myRemote = k2::RPC().getTXEndpoint(serverURL);
    if (!myRemote) {
        K2LOG_E(log::tsoclient, "Invalid server url: {}", serverURL);
        return seastar::make_exception_future(std::runtime_error("invalid server url"));
    }
    auto retryStrategy = seastar::make_lw_shared<k2::ExponentialBackoffStrategy>();
    retryStrategy->withRetries(5).withStartTimeout(1s).withRate(5);

    return retryStrategy->run([this, myRemote=std::move(myRemote)](size_t retriesLeft, k2::Duration timeout)
    {
        K2LOG_I(log::tsoclient, "Sending with retriesLeft={}, and timeout={}ms, with {}",
                retriesLeft, k2::msec(timeout).count(), myRemote->url);
        if (_stopped)
        {
            K2LOG_I(log::tsoclient, "Stopping retry since we were stopped");
            return seastar::make_exception_future<>(TSOClientLibShutdownException());
        }

        GetTSOServiceNodeURLsRequest request;  // empty request param

        return k2::RPC().callRPC<dto::GetTSOServiceNodeURLsRequest, dto::GetTSOServiceNodeURLsResponse>(dto::Verbs::GET_TSO_SERVICE_NODE_URLS, request, *myRemote, timeout)
        .then([this](auto&& response) {
            if (_stopped) return seastar::make_ready_future<>();

            auto& [status, r] = response;
            if (!status.is2xxOK())
            {
                K2LOG_E(log::tsoclient, "Error during get TSO node URLs, status:{}", status);
                // currently, it is not expected
                return seastar::make_exception_future<>(std::runtime_error(status.message));
            }

            auto& nodeURLs = r.serviceNodeURLs;
            if (nodeURLs.empty())
            {
                K2LOG_E(log::tsoclient, "Remote end did not provide node URLs. Giving up");
                return seastar::make_exception_future<>(std::runtime_error("no remote endpoint"));
            }
            else
            {
                K2LOG_I(log::tsoclient, "received node URLs:{}", nodeURLs);
            }

            _curTSOServiceNodes.clear();
            // each node may have mulitple endPoints URLs, we only pick the fastest supported one, currently RDMA, if no RDMA, pick TCPIP
            for (auto& singleNodeURLs : nodeURLs)
            {
                _curTSOServiceNodes.push_back( Discovery::selectBestEndpoint(singleNodeURLs));
                K2LOG_I(log::tsoclient, "Selected node endpoint:{}", _curTSOServiceNodes.back()->url);
            }

            K2ASSERT(log::tsoclient, !_curTSOServiceNodes.empty(), "nodes should property configured and not empty!")

            // to reduce run-time computation, we shuffle the _curTSOServiceNodes here
            // to simulate random pick of workers(load balance) in run time by increment a moded index
            std::random_device rd;
            std::mt19937 ranAlg(rd());

            std::shuffle(_curTSOServiceNodes.begin(), _curTSOServiceNodes.end(), ranAlg);

            // set ready to serve requests
            _readyToServe = true;
            for (auto&& readyPromise : _promiseReadyToServe)
            {
                readyPromise.set_value();
            }
            // we have signaled any waiting request so we should free the memory now.
            _promiseReadyToServe.clear();
            K2LOG_I(log::tsoclient, "Successfully getting remote data endpoint, ready to serve.");

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
        K2LOG_I(log::tsoclient, "Finished getting remote data endpoint");
    });
}

seastar::future<Timestamp> TSO_ClientLib::getTimestampFromTSO(const TimePoint& requestLocalTime)
{
    if (_stopped)
    {
        K2LOG_I(log::tsoclient, "Stopping issuing timestamp since we were stopped");
        return seastar::make_exception_future<Timestamp>(TSOClientLibShutdownException());
    }

    // TSO client may not yet ready (discover the tso server endpoint), let the request wait in this case.
    if (!_readyToServe)
    {
        // if not ready to serve yet, wait on a new ready promise then call get this function self, as each promise can only chain one then lamda
        K2LOG_W(log::tsoclient, "TSO Timestamp requested when not ready to serve, request pending...");
        _promiseReadyToServe.emplace_back();
        return _promiseReadyToServe.back().get_future()
            .then([this, triggeredTime = requestLocalTime] { return getTimestampFromTSO(triggeredTime); });
    }


    // step 1/4 - sanity check if we got out of order client timestamp request
    if (requestLocalTime < _lastSeenRequestTime)
    {
        K2ASSERT(log::tsoclient, false, "requestLocalTime {} is older than _lastSeenRequestTime {}", requestLocalTime, _lastSeenRequestTime);
        return seastar::make_exception_future<Timestamp>(TimeStampRequestOutOfOrderException(nsec_count(requestLocalTime), nsec_count(_lastSeenRequestTime)));
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
            K2ASSERT(log::tsoclient, _pendingClientRequests.empty(), "Available timestamp batch when there is pending client request");

            // this batch must still have some timestamp
            K2ASSERT(log::tsoclient, headBatch._usedCount < headBatch._batch.TSCount, "We should not kept used-up batches.");

            // if obsolete, remove it and retry issuing timestamp from next batch at front.
            if (headBatch.expirationTime() < requestLocalTime)
            {
                K2LOG_W(log::tsoclient, "Detected and discarded existing obsolete batch when issuing TS. headBatch.expirationTime() < requestLocalTime.");
                _timestampBatchQue.pop_front();
                continue;
            }

            // we are here means that the headBatch has timestamp ready to issue
            Timestamp result = TimestampBatch::generateTimeStampFromBatch(headBatch._batch, headBatch._usedCount);
            headBatch._usedCount++;
            K2LOG_D(log::tsoclient, "Issued TS from existing batch.");
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
        K2ASSERT(log::tsoclient, !(_timestampBatchQue.back()._isAvailable), "The last batch should still not coming back yet!");
        K2ASSERT(log::tsoclient, !(_timestampBatchQue.front()._isAvailable), "The first batch, actually every batch, should still not coming back yet!");
        auto& backBatch = _timestampBatchQue.back();
        // check if we can piggy back the last batch that is not back yet, the condition is
        // a) The last batch expected TTL include current request time
        // b) Then number of pending client requests for the last batch is smaller than the batch size
        bool canPiggyBack = (nsec_count(backBatch._triggeredTime) + backBatch._expectedTTL) > nsec_count(requestLocalTime);
        if (canPiggyBack)         // TTL is ok, now check pending count
        {
            uint16_t pendingRequestCountForBackBatch = 0;
            for(auto it = _pendingClientRequests.crbegin(); it != _pendingClientRequests.crend(); it++)
            {
                //K2ASSERT(log::tsoclient, it->_requestTime >= backBatch._triggeredTime, "Outgoing batch request must started before the client request.");

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
            K2LOG_D(log::tsoclient, "Piggy Back on outgoing batch.");
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

    (void) _getTimestampBatch(batchSizeToRequest)
        .then([this, triggeredTime = requestLocalTime](TimestampBatch&& newBatch) {
            _processReturnedBatch(std::move(newBatch), triggeredTime);
        }).handle_exception([this] (auto exc) {
            // Set exception for all pending client requests
            for (auto&& clientRequest : _pendingClientRequests)
            {
                clientRequest._promise->set_exception(exc);
            }
            _pendingClientRequests.clear();

            K2LOG_W_EXC(log::tsoclient, exc, "GetTimestampBatch failed");
        });

    K2LOG_D(log::tsoclient, "Request new Batch for this TS.");

    curRequest._triggeredBatchRequest = true;
    _pendingClientRequests.push_back(std::move(curRequest));
    return _pendingClientRequests.back()._promise->get_future();
}

void TSO_ClientLib::_processReturnedBatch(TimestampBatch batch, TimePoint batchTriggeredTime)
{
    if (_stopped)
    {
        K2LOG_I(log::tsoclient, "Stopping process timestampbatch since we were stopped");
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
        K2LOG_W(log::tsoclient, "TimestampBatch comes in out of order, discarded");
        return;
    }
    bool hasPendingCR= !_pendingClientRequests.empty();
    TimePoint minTimePointBar = _pendingClientRequests.empty()? Clock::now() : _pendingClientRequests.front()._requestTime;
    if(nsec_count(batchTriggeredTime) + batch.TTLNanoSec < nsec_count(minTimePointBar))
    {
        //TODO: log more detailed infor
        K2LOG_W(log::tsoclient, "TimestampBatch comes in late, discarded. hasPendingClientRequest: {}",(hasPendingCR ? "TRUE" : "FALSE"));
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
        ite->expirationTime() < minTimePointBar)
    {
        K2ASSERT(log::tsoclient, ite->_usedCount < ite->_batch.TSCount, "we should not have used-up batch still kept around!");
        K2LOG_D(log::tsoclient, "Discard existing obosolete available Front batch.");

        _timestampBatchQue.pop_front();
        ite = _timestampBatchQue.begin();
    }
    // remove case b)
    ite = _timestampBatchQue.begin();
    while (ite != _timestampBatchQue.end() &&
        !ite->_isAvailable &&
        ite->_triggeredTime < batchTriggeredTime)
    {
        K2LOG_D(log::tsoclient, "Discard existing unavailable older Front batch.");
        _timestampBatchQue.pop_front();
        ite = _timestampBatchQue.begin();
    }
    // now match it, if we don't find a match, this must be a bug. But we can still use it, so log error and insert it in production and crash in debug.
    K2ASSERT(log::tsoclient, ite != _timestampBatchQue.end(), "")

    if (ite == _timestampBatchQue.end() || ite->_triggeredTime > batchTriggeredTime)
    {
        // above Assert should crash in debug build, but in production, let's allow this batch
        K2LOG_W(log::tsoclient, "A valid batch returned but its shell was unexpected removed already!");
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
        K2ASSERT(log::tsoclient, ite->_triggeredTime == batchTriggeredTime, "Find the original shell of the batch in _timestampBatchQue");
        K2ASSERT(log::tsoclient, ite->_isAvailable == false && ite->_usedCount == 0, "the batch was not available till now.")
        ite->_batch = batch;
        ite->_isAvailable = true;
    }

    // step 3/4 if any pending client request in _pendingClientRequests, start to fulfil them in order with the existing batch(es)
    if (!_pendingClientRequests.empty())
    {
        // there are pending client request, in our design, we now can have only one available batch at the front of _timestampBatchQue,
        //as we aggressively fulfill client request when client request arrives or batch comes back, so execpt current incoming batch,
        // we can't have other available batch in _timestampBatchQue.
        K2ASSERT(log::tsoclient, _timestampBatchQue.size() == 1 || !_timestampBatchQue[1]._isAvailable, "We don't expect other available batch!");

        auto& batchInfo = _timestampBatchQue.front();
        // update _lastIssuedBatchTriggeredTime as we are about to issue from this batch
        _lastIssuedBatchTriggeredTime = _lastIssuedBatchTriggeredTime < batchInfo._triggeredTime ? batchInfo._triggeredTime : _lastIssuedBatchTriggeredTime;

        // fulfill as much pending client request as possible, while delete fulfilled pending request
        while (batchInfo._usedCount < batchInfo._batch.TSCount && !_pendingClientRequests.empty())
        {
            if(batchInfo.expirationTime() < _pendingClientRequests.front()._requestTime) {
                K2LOG_D(log::tsoclient, "Skipping an existing obsolete batch.");
                break;
            }

            _pendingClientRequests.front()._promise->set_value(TimestampBatch::generateTimeStampFromBatch(batchInfo._batch, batchInfo._usedCount));
            _pendingClientRequests.pop_front();
            batchInfo._usedCount++;
        }

       // TODO: optimize this to keep the current front batch if there is still valid TS in it.
        _timestampBatchQue.pop_front();
    }

    // step 4/4 if all available batches are used up and existing unavailable/outgoing batches is not enough to fulfill all the pending client request
    // issue replacement batch request
    if (!_pendingClientRequests.empty())
    {
    uint16_t pendingClientRequestsCount = (uint16_t) _pendingClientRequests.size();
        uint16_t expectedTSCount = 0;
        uint16_t batchSizeToRequest = 0;
        const auto& cTimestampBatchQue = _timestampBatchQue;
        for (auto&& batchInfo : cTimestampBatchQue)
        {
            K2ASSERT(log::tsoclient, !batchInfo._isAvailable, "We should not have available batch not fulfilled to client request");
            expectedTSCount += batchInfo._expectedBatchSize;
        }

        batchSizeToRequest = expectedTSCount >= pendingClientRequestsCount ? 0 : pendingClientRequestsCount - expectedTSCount;

        if (batchSizeToRequest > 0)
        {
            K2LOG_D(log::tsoclient, "Need to request more batch due to unfulfilled pending client requests, count: {}", batchSizeToRequest);
            // TODO: get config from appBase and use max batch size, default 32
            batchSizeToRequest = std::min(batchSizeToRequest, (uint16_t)32);

            TimePoint curTime = Clock::now();
            TimestampBatchInfo newBatchRequest;
            newBatchRequest._triggeredTime = curTime;
            newBatchRequest._expectedBatchSize = batchSizeToRequest;
            newBatchRequest._expectedTTL = 8000;    // in nanosecond, TODO: use config value instead.
            newBatchRequest._isTriggeredByReplacement = true;  // this is a replacement
            _timestampBatchQue.emplace_back(std::move(newBatchRequest));

            (void) _getTimestampBatch(batchSizeToRequest)
                .then([this, triggeredTime = curTime](TimestampBatch newBatch) {
                    _processReturnedBatch(newBatch, triggeredTime);
            }).handle_exception([this] (auto exc) {
                // Set exception for all pending client requests
                for (auto&& clientRequest : _pendingClientRequests)
                {
                    clientRequest._promise->set_exception(exc);
                }
                _pendingClientRequests.clear();

                K2LOG_W_EXC(log::tsoclient, exc, "GetTimestampBatch failed");
            });
        }
    }
}

seastar::future<TimestampBatch> TSO_ClientLib::_getTimestampBatch(uint16_t batchSize)
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
                K2LOG_I(log::tsoclient, "Stopping retry since we were stopped");
                return seastar::make_exception_future<>(TSOClientLibShutdownException());
            }

            K2ASSERT(log::tsoclient, !_curTSOServiceNodes.empty(), "we should have workers");
            // pick next worker (effecitvely random one, as _curTSOServiceNodes is shuffled already when it is populated)
            if (retriesLeft != 2) {_curWorkerIdx++;}  // if this is not first try, it means we had error and are retrying, thus change to a new service node.
            int randNode = _curWorkerIdx %  _curTSOServiceNodes.size();
            auto& myRemote = _curTSOServiceNodes[randNode];

            GetTimeStampBatchRequest request{.batchSizeRequested = batchSize};
            K2LOG_D(log::tsoclient, "Requesting timestampBatch of batchsize:{} with retriesLeft:{} and timeout:{} to node:{}", batchSize, retriesLeft, timeout, randNode);

            return k2::RPC().callRPC<dto::GetTimeStampBatchRequest, dto::GetTimeStampBatchResponse>(dto::Verbs::GET_TSO_TIMESTAMP_BATCH, request, *myRemote, timeout)
            .then([this, &batch](auto&& response) {
                if (_stopped)
                {
                    K2LOG_I(log::tsoclient, "Stopping retry since we were stopped");
                    return seastar::make_exception_future<>(TSOClientLibShutdownException());
                }

                auto& [status, r] = response;
                if (!status.is2xxOK())
                {
                    K2LOG_E(log::tsoclient, "Error during get timestampBatch, status:{}", status);
                    // currently, we should only have 5xx retryable error, assert to confirm here to make sure future other error status added is handled.
                    K2ASSERT(log::tsoclient, status.is5xxRetryable(), "GetTimeStampBatch error should be 5xxRetryable.");
                    return seastar::make_exception_future<>(std::runtime_error(status.message));
                }

                K2LOG_V(log::tsoclient, "got timestampBatch:{}", r.timeStampBatch);
                batch = r.timeStampBatch;
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
