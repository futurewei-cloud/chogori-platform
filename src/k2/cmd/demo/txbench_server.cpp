//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->

// stl
#include <cstdlib>
#include <ctime>
#include <unordered_map>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>

#include "txbench_common.h"

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
        k2::RPC().registerMessageObserver(GET_DATA_URL, nullptr);
        k2::RPC().registerMessageObserver(REQUEST, nullptr);
        k2::RPC().registerMessageObserver(START_SESSION, nullptr);
        k2::RPC().registerLowTransportMemoryObserver(nullptr);
        return seastar::make_ready_future<>();
    }

    seastar::future<> start() {
        _stopped = false;

        _registerDATA_URL();
        _registerSTART_SESSION();
        _registerREQUEST();

        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: "<< ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });
        return seastar::make_ready_future<>();
    }

private:
    void _registerDATA_URL() {
        K2INFO("TCP endpoint is: " << k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto)->getURL());
        k2::RPC().registerMessageObserver(GET_DATA_URL,
            [this](k2::Request&& request) mutable {
                auto response = request.endpoint.newPayload();
                auto ep = (seastar::engine()._rdma_stack?
                           k2::RPC().getServerEndpoint(k2::RRDMARPCProtocol::proto):
                           k2::RPC().getServerEndpoint(k2::TCPRPCProtocol::proto));
                K2INFO("GET_DATA_URL responding with data endpoint: " << ep->getURL());
                response->getWriter().write((void*)ep->getURL().c_str(), ep->getURL().size());
                k2::RPC().sendReply(std::move(response), request);
            });
    }

    void _registerSTART_SESSION() {
        k2::RPC().registerMessageObserver(START_SESSION, [this](k2::Request&& request) mutable {
            auto sid = uint64_t(std::rand());
            if (request.payload) {
                auto rdr = request.payload->getReader();
                SessionConfig config{};
                rdr.read((void*)&config, sizeof(config));

                BenchSession session(request.endpoint, sid, config);
                auto result = _sessions.try_emplace(sid, std::move(session));
                assert(result.second);
                K2INFO("Starting new session: " << sid);
                auto resp = request.endpoint.newPayload();
                SessionAck ack{.sessionID=sid};
                resp->getWriter().write((void*)&ack, sizeof(ack));
                k2::RPC().sendReply(std::move(resp), request);
            }
        });
    }

    void _registerREQUEST() {
        k2::RPC().registerMessageObserver(REQUEST, [this](k2::Request&& request) mutable {
            if (request.payload) {
                auto rdr = request.payload->getReader();
                uint64_t sid = 0;
                uint64_t reqId = 0;
                rdr.read((void*)&sid, sizeof(sid));
                rdr.read((void*)&reqId, sizeof(reqId));

                auto siditer = _sessions.find(sid);
                if (siditer != _sessions.end()) {
                    auto& session = siditer->second;
                    session.runningSum += reqId;
                    session.totalSize += session.config.responseSize;
                    session.totalCount += 1;
                    session.unackedSize += session.config.responseSize;
                    session.unackedCount += 1;
                    if (session.config.echoMode) {
                        auto&& buffs = request.payload->release();
                        _data.insert(_data.end(), std::move_iterator(buffs.begin()), std::move_iterator(buffs.end()));
                    }
                    if (session.unackedCount >= session.config.ackCount) {
                        auto response = request.endpoint.newPayload();
                        Ack ack{.sessionID=sid, .totalCount=session.totalCount, .totalSize=session.totalSize, .checksum=session.runningSum};
                        response->getWriter().write((void*)&ack, sizeof(ack));
                        if (session.config.echoMode) {
                            for (auto&& buf: _data) {
                                response->getWriter().write(buf.get(), buf.size());
                            }
                            _data.clear();
                        }
                        k2::RPC().send(ACK, std::move(response), request.endpoint);
                        session.unackedSize = 0;
                        session.unackedCount = 0;
                    }
                }
            }
        });
    }

private:
    // flag we need to tell if we've been stopped
    bool _stopped;
    std::unordered_map<uint64_t, BenchSession> _sessions;
    std::vector<k2::Binary> _data;
}; // class Service

int main(int argc, char** argv) {
    std::srand(std::time(nullptr));
    k2::App app;
    app.addActivity<Service>();
    return app.start(argc, argv);
}
