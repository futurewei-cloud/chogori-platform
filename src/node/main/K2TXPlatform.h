//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

// k2
#include "common/Log.h"
#include "common/Common.h"
#include "transport/RPCDispatcher.h"
#include "transport/TCPRPCProtocol.h"
#include "transport/RPCProtocolFactory.h"
#include "transport/VirtualNetworkStack.h"
#include "node/AssignmentManager.h"
#include "node/NodePool.h"

namespace k2 {

// This class allows us to run a NodePool as a distributed<> service. It essentially wraps
// an AssignmentManager, making sure we have one assignment manager per core
class NodePoolService {
public: // types
    typedef seastar::distributed<NodePoolService> Dist_t;

public:
    NodePoolService(NodePool& pool, k2::RPCDispatcher::Dist_t& dispatcher);
    ~NodePoolService();

public: // distributed<> interface
    // required for seastar::distributed interface
    seastar::future<> stop();

    // called after construction
    void Start();

private: // helpers
    seastar::future<> startTaskProcessor();

private: // fields
    AssignmentManager _assignmentManager;
    bool _stopped;
    seastar::future<> _taskProcessorLoop = seastar::make_ready_future<>();
    k2::RPCDispatcher::Dist_t& _dispatcher;

private: // don't need
    NodePoolService() = delete;
    NodePoolService(const NodePoolService& o) = delete;
    NodePoolService(NodePoolService&& o) = delete;
    NodePoolService& operator=(const NodePoolService& o) = delete;
    NodePoolService& operator=(NodePoolService&& o) = delete;
}; // class NodePoolService

// This class provides the ISchedulingPlatform interface and takes care of
// running a NodePool over tx transport
class K2TXPlatform : public ISchedulingPlatform {
public: //lifecycle
    K2TXPlatform();
    ~K2TXPlatform();

    // this method runs nodes based on the configuration provided in the given NodePool configurator
    // The method is not expected to return until signal(e.g. SIGTERM) or a fatal error occurs
    Status run(NodePool& pool);

public: // ISchedulingPlatform interface
    // return a node identifier
    uint64_t getCurrentNodeId() override {
        return seastar::engine().cpu_id();
    }

    // run the given function after the given delayTime
    void delay(std::chrono::microseconds delayTimeUs, std::function<void()>&& callback) override {
        seastar::sleep(delayTimeUs).then([cb = std::move(callback)] { cb(); });
    }

private: // don't need
    K2TXPlatform(const K2TXPlatform& o) = delete;
    K2TXPlatform(K2TXPlatform&& o) = delete;
    K2TXPlatform& operator=(const K2TXPlatform& o) = delete;
    K2TXPlatform& operator=(K2TXPlatform&& o) = delete;
}; // class K2TXPlatform

} //  namespace k2
