//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <seastar/core/distributed.hh>

namespace k2tx {

// This class allows access to the proper VirtualFunction(SR-IOV virtualized NIC) on a given core
class VirtualFunction{
public:
    // ctor
    VirtualFunction() = default;
    VirtualFunction(const VirtualFunction &o) = default;
    VirtualFunction(VirtualFunction &&o) = default;
    VirtualFunction &operator=(const VirtualFunction &o) = default;
    VirtualFunction &operator=(VirtualFunction &&o) = default;
    virtual ~VirtualFunction();

    // Use to determine the ID for the VF on the core on which this instance is running
    unsigned GetVFID() const;

    // this method will be called once all objects in the distributed system have been wired
    void Start();

    // (seastar::distributed interface) this method will be called when the object is about to be destroyed
    seastar::future<> stop();

    // Distributed version of the class
    typedef seastar::distributed<VirtualFunction> Dist_t;

}; // class VirtualFunction

}// namespace
