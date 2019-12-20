//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

// third-party
#include <seastar/core/distributed.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

// k2
#include <k2/common/Log.h>
#include <k2/common/Common.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/TCPRPCProtocol.h>
#include <k2/transport/RPCProtocolFactory.h>
#include <k2/transport/VirtualNetworkStack.h>
#include <k2/transport/Prometheus.h>
#include <k2/node/NodePoolImpl.h>

namespace k2 {

// This class provides the ISchedulingPlatform interface and takes care of
// running a NodePool over tx transport
class K2TXPlatform : public ISchedulingPlatform
{
protected:
    k2::Prometheus _prometheus;
    const uint16_t _defaultPrometheusPort = 8089;

public:
    K2TXPlatform() {}

    // this method runs nodes based on the NodePool configuration
    // The method is not expected to return until signal(e.g. SIGTERM) or a fatal error occurs
    Status run(NodePoolImpl& pool);

    //
    //  ISchedulingPlatform interface
    //
    uint64_t getCurrentNodeId() override { return seastar::engine().cpu_id(); } // return a node identifier

    // run the given function after the given delayTime
    void delay(Duration delayTime, std::function<void()>&& callback) override {
        (void) seastar::sleep(delayTime).then([cb = std::move(callback)] { cb(); });
    }

    DISABLE_COPY_MOVE(K2TXPlatform)
}; // class K2TXPlatform

} //  namespace k2
