//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include <seastar/core/sleep.hh>

#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/RPCDispatcher.h>

#include "CPOClient.h"

namespace k2 {

CPOClient::CPOClient(String cpo_url) {
    cpo = RPC().getTXEndpoint(cpo_url);
}

void CPOClient::FulfillWaiters(const String& name, const Status& status) {
    auto& waiters = requestWaiters[name];

    for (auto it = waiters.begin(); it != waiters.end(); ++it) {
        it->set_value(status);
    }

    requestWaiters.erase(name);
}

// Get collection info from CPO, and retry if the partition for the given key
// is not assigned or if there was a retryable error. It allows only one outstanding
// request for a given collection.
seastar::future<Status> CPOClient::GetAssignedPartitionWithRetry(Deadline<> deadline, const String& name, const dto::Key& key, uint8_t retries) {
    // Check if request is already issued, if so add to waiters and return
    auto it = requestWaiters.find(name);
    if (it != requestWaiters.end()) {
        it->second.emplace_back(seastar::promise<Status>());
        return it->second.back().get_future()
        .then([this, deadline, name, key, retries] (Status&& status) {
            if (status.is2xxOK()) {
                dto::Partition* partition = collections[name].getPartitionForKey(key);
                if (partition && partition->astate == dto::AssignmentState::Assigned) {
                    return seastar::make_ready_future<Status>(std::move(status));
                }
            }

            if (!retries) {
                status = Status::S408_Request_Timeout("Retries exceeded");
                return seastar::make_ready_future<Status>(std::move(status));
            }

            return GetAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), retries-1);
        });
    }

    // Register the ongoing request
    requestWaiters[name] = std::vector<seastar::promise<Status>>();

    Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
    dto::CollectionGetRequest request{.name = name};

    return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET,                 request, *cpo, timeout).
    then([this, name=request.name, key, deadline, retries] (auto&& response) {
        auto& [status, coll_response] = response;
        bool retry = false;

        if (status.is2xxOK()) {
            collections[name] = dto::PartitionGetter(std::move(coll_response.collection));
            dto::Partition* partition = collections[name].getPartitionForKey(key);
            FulfillWaiters(name, status);
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                K2DEBUG("No partition or not assigned: " << partition);
                retry = true;
            }
        } else if (status.is5xxRetryable()) {
            retry = true;
        } else {
            FulfillWaiters(name, status);
            return seastar::make_ready_future<Status>(std::move(status));
        }

        if (!retry) {
            return seastar::make_ready_future<Status>(std::move(status));
        }

        if (status.is2xxOK() && retry && !retries) {
            status = Status::S503_Service_Unavailable("Not all partitions assigned");
            FulfillWaiters(name, status);
            return seastar::make_ready_future<Status>(std::move(status));
        }

        if (deadline.isOver()) {
            status = Status::S408_Request_Timeout("Deadline exceeded");
            FulfillWaiters(name, status);
            return seastar::make_ready_future<Status>(std::move(status));
        }
    
        if (!retries) {
            FulfillWaiters(name, status);
            status = Status::S408_Request_Timeout("Retries exceeded");
            return seastar::make_ready_future<Status>(std::move(status));
        }

        Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
        return seastar::sleep(s).
        then([this, name, key, deadline, retries] () -> seastar::future<Status> {
            return GetAssignedPartitionWithRetry(deadline, std::move(name), std::move(key), retries-1);
        });
    });
}

// Creates a collection and waits for at least one partition to  be assigned. If the collection 
// already exisits, the future is still completed successfully
seastar::future<Status> CPOClient::CreateAndWaitForCollection(Deadline<> deadline, dto::CollectionMetadata&& metadata, std::vector<String>&& clusterEndpoints) {
    dto::CollectionCreateRequest request{.metadata = std::move(metadata), 
                                         .clusterEndpoints = std::move(clusterEndpoints)};

    Duration timeout = std::min(deadline.getRemaining(), cpo_request_timeout());
    return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE , request, *cpo, timeout).
    then([this, name=request.metadata.name, deadline] (auto&& response) {
        auto& [status, k2response] = response;

        if (status == Status::S403_Forbidden() || status.is2xxOK()) {
            Duration s = std::min(deadline.getRemaining(), cpo_request_backoff());
            return seastar::sleep(s).
            then([this, name, deadline] () -> seastar::future<Status> {
                return GetAssignedPartitionWithRetry(deadline, name, dto::Key{.partitionKey="", .rangeKey=""});
            });

        }

        return seastar::make_ready_future<Status>(std::move(status));
    });
}

} // ns k2
