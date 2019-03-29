//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <memory> // ptr stuff

#include <seastar/core/distributed.hh> // seastar's distributed/sharded stuff
#include <functional>
#include "Listener.h"

namespace k2tx {
class ListenerFactory {
public:
    // Distributed version of the class
    typedef seastar::distributed<ListenerFactory> Dist_t;

    // Factory signature for building Listeners
    typedef std::function<std::shared_ptr<Listener>()> BuilderFunc_t;

    // Make a new ListenerFactory with the given Listener builder
    ListenerFactory(BuilderFunc_t builder);

    // dtor
    ~ListenerFactory();

    // Obtain the Listener instance
    std::shared_ptr<Listener> ins() { return _instance;}

    // Should be called by user when all distributed objects have been created
    void Start();

    // called by seastar's distributed mechanism when stop() is invoked on the distributed container.
    seastar::future<> stop();

private:
    // the factory for building a listener
    BuilderFunc_t _builder;

    // this is where we keep the instance returned by the factory
    std::shared_ptr<Listener> _instance;
}; // ListenerFactory

} // k2tx
