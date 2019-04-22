//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
// stl
#include <functional> // functions

// third-party
#include <seastar/core/shared_ptr.hh> // seastar's pointers
#include <seastar/core/distributed.hh> // seastar's distributed/sharded stuff

// k2tx
#include "IRPCProtocol.h"

namespace k2tx {

// This is a helper class used to construct concrete protocols of a given type on demand in the process
// of building a distributed<> object.
// This class should be used as a distributed<> container
class RPCProtocolFactory {

public: // types
    // Distributed version of the class
    typedef seastar::distributed<RPCProtocolFactory> Dist_t;

    // Factory signature for building protocols
    typedef std::function<seastar::shared_ptr<IRPCProtocol>()> BuilderFunc_t;

public: // lifecycle
    // Make a new RPCProtocolFactory with the given IRPCProtocol builder. This builder should produce
    // instances of concrete protocols (e.g. TCPRPCProtocol)
    RPCProtocolFactory(BuilderFunc_t builder);

    // dtor
    ~RPCProtocolFactory();

public: // interface
    // Obtain the IRPCProtocol instance
    seastar::shared_ptr<IRPCProtocol> instance() { return _instance;}

public: // distributed<> interface
    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

    // Should be called by user when all distributed objects have been created
    void Start();

private:
    // the factory for building a protocol
    BuilderFunc_t _builder;

    // this is where we keep the instance returned by the factory
    seastar::shared_ptr<IRPCProtocol> _instance;
}; // RPCProtocolFactory

} // k2tx
