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

seastar::future<Status> CPOClient::GetAssignedPartitionWithRetry(Deadline<> deadline, const String& name, const dto::Key& key, uint8_t retries) {
    // Check if request is already issued, if so add to waiters and return
    auto it = requestWaiters.find(name);
    if (it != requestWaiters.end()) {
        it->second.emplace_back(seastar::promise<Status>());
        return it->second.back().get_future();
    }

    // Register the ongoing request
    requestWaiters[name] = std::vector<seastar::promise<Status>>();

    Duration timeout = std::min(deadline.getRemaining(), Duration(100ms));
    dto::CollectionGetRequest request{.name = std::move(name)};

    return RPC().callRPC<dto::CollectionGetRequest, dto::CollectionGetResponse>(dto::Verbs::CPO_COLLECTION_GET,                 std::move(request), *cpo, timeout).
    then([this, name=request.name, key, deadline, retries] (auto&& response) {
        auto& [status, coll_response] = response;
        bool retry = false;

        if (status.is2xxOK()) {
            collections[name] = dto::PartitionGetter(std::move(coll_response.collection));
            dto::Partition* partition = collections[name].getPartitionForKey(key);
            FulfillWaiters(name, status);
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
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
            return seastar::make_ready_future<Status>(std::move(status));
        }

        Duration s = std::min(deadline.getRemaining(), Duration(500ms));
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

    Duration timeout = std::min(deadline.getRemaining(), Duration(100ms));
    return RPC().callRPC<dto::CollectionCreateRequest, dto::CollectionCreateResponse>(dto::Verbs::CPO_COLLECTION_CREATE , std::move(request), *cpo, timeout).
    then([this, name=request.metadata.name, deadline] (auto&& response) {
        auto& [status, k2response] = response;

        if (status == Status::S403_Forbidden() || status.is2xxOK()) {
            return GetAssignedPartitionWithRetry(deadline, name, dto::Key{.partitionKey="", .rangeKey=""});
        }

        return seastar::make_ready_future<Status>(std::move(status));
    });
}

// Gets the partition endpoint for request's key, executes the request, and refreshes the 
// partition map if necessary. The caller must keep the request alive for the duration of the
// future.
// RequestT must have a pvid field and a collectionName field
template<class RequestT, typename ResponseT, Verb verb>
seastar::future<std::tuple<Status, ResponseT>> CPOClient::PartitionRequest(Deadline<> deadline, RequestT& request, uint8_t retries) {
    // If collection is not in cache or partition is not assigned, get collection first
    seastar::future<Status> f = seastar::make_ready_future<Status>(Status::S200_OK());
    auto it = collections.find(request.collectionName);
    if (it == collections.end()) {
        f = GetAssignedCollectionWithRetry(deadline, request.collectionName, request.key);
    } else {
        dto::Partition* partition = collections[request.collectionName].getPartitionForKey(request.key);
        if (!partition || partition->astate != dto::AssignmentState::Assigned) {
            f = GetAssignedCollectionWithRetry(deadline, request.collectionName);
        }
    }

    return f.then([this, deadline, &request] (Status&& status) {
        // Get partition info for request

        if (deadline.isOver()) {
            status = Status::S408_Request_Timeout("Deadline exceeded");
            return seastar::make_ready_future<std::tuple<Status, dto::Partition*>>(
                        std::make_tuple(std::move(status), nullptr));
        }

        auto it = collections.find(request.collectionName);
        if (it == collections.end()) {
            return seastar::make_ready_future<std::tuple<Status, dto::Partition*>>(
                        std::make_tuple(std::move(status), nullptr));
        }

        dto::Partition* partition = collections[request.collectionName].getPartitionForKey(request.key);
        if (!partition || partition->astate != dto::AssignmentState::Assigned) {
            status = Status::S503_Service_Unavailable("Partition not assigned");
            return seastar::make_ready_future<std::tuple<Status, dto::Partition*>>(
                        std::make_tuple(std::move(status), nullptr));
        }

        return seastar::make_ready_future<std::tuple<Status, dto::Partition*>>(
                    std::make_tuple(std::move(status), partition));

    }).then([this, deadline, &request, retries] (auto&& status_partition) {
        auto& [status, partition] = status_partition;

        // Partition is still not assigned after two refresh attempts
        if (!partition) {
            return seastar::make_ready_future<std::tuple<Status, ResponseT>>(
                        std::make_tuple(std::move(status), ResponseT()));
        }

        Duration timeout = std::min(deadline.getRemaining(), Duration(1ms));
        TXEndpoint k2node(partition->endpoints.begin());
        request.pvid = partition->pvid;

        // Attempt the request RPC
        return RPC().callRPC<RequestT, ResponseT>(verb, std::move(request), std::move(k2node), timeout).
        then([this, &request, deadline, retries] (auto&& result) {
            auto& [status, k2response] = result;

            // Success or unrecoverable error
            if (status != Status::S410_Gone() && (status.is2xxOK() || !status.is5xxRetryable())) {
                return seastar::make_ready_future<std::tuple<Status, ResponseT>>(std::make_tuple(
                    std::move(status), std::move(k2response)));
            }

            if (deadline.isOver()) {
                status = Status::S408_Request_Timeout("Deadline exceeded");
                return seastar::make_ready_future<std::tuple<Status, ResponseT>>(
                            std::make_tuple(std::move(status), ResponseT()));
            }

            if (retries == 0) {
                return seastar::make_ready_future<std::tuple<Status, ResponseT>>(std::make_tuple(
                        Status::S408_Request_Timeout("Retries exceeded"), ResponseT()));
            }

            // S410_Gone (refresh partition map) or retryable error
            return GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key, 1)
            .then([this, &request, deadline, retries] (Status&& status) {
                return PartitionRequest<RequestT, ResponseT, verb>(deadline, request, retries-1);
            });
        });
    });
}

} // ns k2
