//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once

#include <seastar/core/distributed.hh>

#include "VirtualFunction.h"

namespace k2tx {
class Listener {
public:
    Listener(VirtualFunction::Dist_t& vfs);
    Listener(const Listener& o) = default;
    Listener(Listener&& o) = default;
    Listener& operator=(const Listener& o) = default;
    Listener& operator=(Listener&& o) = default;
    virtual ~Listener();

    virtual void Start() = 0;
    virtual seastar::future<> stop() = 0;

   private:
    VirtualFunction::Dist_t& _vfs;
};

} // namespace k2tx
