//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <cstdlib>
#include <ctime>
#include <unordered_map>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>
#include <k2/transport/Status.h>

#include "mock_k23si_client.h"

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
        k2::RPC().registerMessageObserver(MessageVerbs::GET_DATA_URL, nullptr);
        k2::RPC().registerMessageObserver(MessageVerbs::GET, nullptr);
        k2::RPC().registerMessageObserver(MessageVerbs::PUT, nullptr);
        k2::RPC().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;

        K2INFO("Registering message handlers");
        _registerDATA_URL();

        RPC().registerRPCObserver<PUT_Request, PUT_Response>(MessageVerbs::PUT, [this](PUT_Request&& request) {
            K2DEBUG("Received put for key: " << request.key);
            String hash_key = request.key.partition_key + request.key.row_key;

            total += hash_key.size() + request.value.getCapacity() + sizeof(request.value);

            _data[hash_key] = request.value.copy();
            PUT_Response response{.key=std::move(request.key)};

            if (total % 100000 == 0) {
                K2INFO("Wrote " << total / 1024.0 << " KB");
                K2INFO(_data.size() << " records in _data, load factor: " << _data.load_factor());
            }
            return RPCResponse(Status::S200_OK(), std::move(response));
        });

        RPC().registerRPCObserver<Key, GET_Response>(MessageVerbs::GET, [this](Key&& key) {
            K2DEBUG("Received get for key: " << key);
            String hash_key = key.partition_key + key.row_key;

            GET_Response response = {};
            key=std::move(key);

            auto iter = _data.find(hash_key);
            if (iter != _data.end()) {
                response.value = iter->second.share();
            }
            return RPCResponse(iter != _data.end() ? Status::S200_OK() : Status::S404_Not_Found(), std::move(response));
        });

        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return seastar::make_ready_future<>();
    }

private:
    void _registerDATA_URL() {
        K2INFO("TCP endpoint is: " << k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->getURL());
        k2::RPC().registerMessageObserver(MessageVerbs::GET_DATA_URL,
            [this](k2::Request&& request) mutable {
                auto response = request.endpoint.newPayload();
                auto ep = (seastar::engine()._rdma_stack?
                           k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto):
                           k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto));
                K2INFO("GET_DATA_URL responding with data endpoint: " << ep->getURL());
                response->write((void*)ep->getURL().c_str(), ep->getURL().size());
                k2::RPC().sendReply(std::move(response), request);
            });
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
