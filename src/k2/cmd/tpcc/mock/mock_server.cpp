//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <cstdlib>
#include <ctime>
#include <unordered_map>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/dto/AssignmentManager.h>
#include <k2/dto/K23SI.h>
#include <k2/dto/Collection.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/transport/Status.h>

using namespace k2;

class Service : public seastar::weakly_referencable<Service> {
public:  // application lifespan
    Service():
        _stopped(true) {
        K2INFO("ctor");
    };

    virtual ~Service() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> stop() {
        K2INFO("stop");
        _stopped = true;
        // unregistar all observers
        k2::RPC().registerMessageObserver(dto::Verbs::K23SI_READ, nullptr);
        k2::RPC().registerMessageObserver(dto::Verbs::K23SI_WRITE, nullptr);
        k2::RPC().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;

        K2INFO("Registering message handlers");

        RPC().registerRPCObserver<dto::K23SIWriteRequest<Payload>, dto::K23SIWriteResponse>(dto::Verbs::K23SI_WRITE, [this](dto::K23SIWriteRequest<Payload>&& request) {
            K2DEBUG("Received put for key: " << request.key);
            String hash_key = request.key.partitionKey + request.key.rangeKey;

            total += hash_key.size() + request.value.val.getCapacity() + sizeof(request.value);

            _data[hash_key] = request.value.val.copy();
            dto::K23SIWriteResponse response{};

            if (total % 100000 == 0) {
                K2INFO("Wrote " << total / 1024.0 << " KB");
                K2INFO(_data.size() << " records in _data, load factor: " << _data.load_factor());
            }
            return RPCResponse(Status::S200_OK(), std::move(response));
        });

        RPC().registerRPCObserver<dto::K23SIReadRequest, dto::K23SIReadResponse<Payload>>(dto::Verbs::K23SI_READ, [this](dto::K23SIReadRequest&& request) {
            K2DEBUG("Received get for key: " << request.key);
            String hash_key = request.key.partitionKey + request.key.rangeKey;

            dto::K23SIReadResponse<Payload> response{};

            auto iter = _data.find(hash_key);
            if (iter != _data.end()) {
                response.value.val = iter->second.share();
            }
            return RPCResponse(iter != _data.end() ? Status::S200_OK() : Status::S404_Not_Found(), std::move(response));
        });

        RPC().registerRPCObserver<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>(dto::Verbs::K23SI_TXN_END,
        [this] (dto::K23SITxnEndRequest&& request) {
            (void) request;
            return RPCResponse(Status::S200_OK(), dto::K23SITxnEndResponse());
        });

        RPC().registerRPCObserver<dto::K23SITxnHeartbeatRequest, dto::K23SITxnHeartbeatResponse>(dto::Verbs::K23SI_TXN_HEARTBEAT,
        [this] (dto::K23SITxnHeartbeatRequest&& request) {
            (void) request;
            return RPCResponse(Status::S200_OK(), dto::K23SITxnHeartbeatResponse());
        });

        RPC().registerRPCObserver<dto::AssignmentCreateRequest, dto::AssignmentCreateResponse>(dto::Verbs::K2_ASSIGNMENT_CREATE,
        [this] (dto::AssignmentCreateRequest&& request) {
            auto ep = (seastar::engine()._rdma_stack?
                       k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto):
                       k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto));
            request.partition.endpoints.clear();
            request.partition.endpoints.insert(ep->getURL());

            dto::AssignmentCreateResponse response{.assignedPartition = std::move(request.partition)};
            response.assignedPartition.astate = dto::AssignmentState::Assigned;

            return RPCResponse(Status::S200_OK(), std::move(response));
        });

        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return seastar::make_ready_future<>();
    }

private:
    // flag we need to tell if we've been stopped
    bool _stopped;
    uint64_t total=0;
    std::unordered_map<String, Payload> _data;
}; // class Service

int main(int argc, char** argv) {
    k2::App app;
    app.addApplet<Service>();
    return app.start(argc, argv);
}
