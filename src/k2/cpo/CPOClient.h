//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->
#pragma once

#include <unordered_map>
#include <vector>
#include <tuple>

#include <seastar/core/future.hh>  // for future stuff

#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>

namespace k2 {

class CPOClient {
public:
    CPOClient(String cpo_url);
    CPOClient() = default;

    // Creates a collection and waits for it to be assigned. If the collection already exisits,
    // the future is still completed successfully
    seastar::future<Status> CreateAndWaitForCollection(Deadline<> deadline, dto::CollectionMetadata&& metadata, std::vector<String>&& clusterEndpoints);

    // Get collection info from CPO, and retry if the partition for the given key
    // is not assigned or if there was a retryable error. It allows only one outstanding
    // request for a given collection.
    seastar::future<Status> GetAssignedPartitionWithRetry(Deadline<> deadline, const String& name, const dto::Key& key, uint8_t retries=1);

    // Gets the partition endpoint for request's key, executes the request, and refreshes the 
    // partition map and retries if necessary. The caller must keep the request alive for the 
    // duration of the future.
    // RequestT must have a pvid field and a collectionName field
    template<class RequestT, typename ResponseT, Verb verb>
    seastar::future<std::tuple<Status, ResponseT>> PartitionRequest(Deadline<> deadline, RequestT& request, uint8_t retries=1) {
        // If collection is not in cache or partition is not assigned, get collection first
        seastar::future<Status> f = seastar::make_ready_future<Status>(Status::S200_OK());
        auto it = collections.find(request.collectionName);
        if (it == collections.end()) {
            f = GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key);
        } else {
            dto::Partition* partition = collections[request.collectionName].getPartitionForKey(request.key);
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                f = GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key);
            }
        }

        return f.then([this, deadline, &request, retries] (Status&& status) {
            auto it = collections.find(request.collectionName);

            if (it == collections.end()) {
                // Failed to get collection, returning status from GetAssignedPartitionWithRetry
                K2DEBUG("Failed to get collection: " << status);
                return RPCResponse(std::move(status), ResponseT());
            }

            // Try to get partition info
            dto::Partition* partition = collections[request.collectionName].getPartitionForKey(request.key);
            if (!partition || partition->astate != dto::AssignmentState::Assigned) {
                // Partition is still not assigned after refresh attempts
                K2DEBUG("Failed to get assigned partition");
                return RPCResponse(Status::S503_Service_Unavailable("Partition not assigned"), ResponseT());
            }

            Duration timeout = std::min(deadline.getRemaining(), partition_request_timeout());
            auto k2node = RPC().getTXEndpoint(*(partition->endpoints.begin()));
            request.pvid = partition->pvid;

            // Attempt the request RPC
            return RPC().callRPC<RequestT, ResponseT>(verb, request, *k2node, timeout).
            then([this, &request, deadline, retries] (auto&& result) {
                auto& [status, k2response] = result;

                // Success or unrecoverable error
                if (status != Status::S410_Gone() && !status.is5xxRetryable()) {
                    return RPCResponse(std::move(status), std::move(k2response));
                }

                if (deadline.isOver()) {
                    K2DEBUG("Deadline exceeded");
                    status = Status::S408_Request_Timeout("Deadline exceeded");
                    return RPCResponse(std::move(status), ResponseT());
                }

                if (retries == 0) {
                    K2DEBUG("Retries exceeded, status: " << status);
                    return RPCResponse(Status::S408_Request_Timeout("Retries exceeded"), ResponseT());
                }

                // S410_Gone (refresh partition map) or retryable error
                return GetAssignedPartitionWithRetry(deadline, request.collectionName, request.key, 1)
                .then([this, &request, deadline, retries] (Status&& status) {
                    (void) status;
                    return PartitionRequest<RequestT, ResponseT, verb>(deadline, request, retries-1);
                });
            });
        });
    }

    std::unique_ptr<TXEndpoint> cpo;
    std::unordered_map<String, dto::PartitionGetter> collections;

    ConfigDuration partition_request_timeout{"partition_request_timeout", 100ms};
    ConfigDuration cpo_request_timeout{"cpo_request_timeout", 100ms};
    ConfigDuration cpo_request_backoff{"cpo_request_backoff", 500ms};
private:
    void FulfillWaiters(const String& name, const Status& status);
    std::unordered_map<String, std::vector<seastar::promise<Status>>> requestWaiters;
};

} // ns k2
