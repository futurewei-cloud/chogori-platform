#pragma once

// std
#include <iostream>
// k2
#include "RootScheduler.h"
#include "SchedulingGroup.h"
#include "ISchedulable.h"

namespace k2
{

//
// Simple TCP Service implementation.
//
class TcpService: public ISchedulable {
public:
    TcpService() {
        // left empty
    }

    ~TcpService() {
        // left empty
    }

    virtual seastar::future<> run() {
        // TODO: Implement socker listening logic.
        return seastar::sleep(std::chrono::milliseconds(500)).then([] {
            return seastar::make_ready_future<>();
        });
    };

}; // class TcpService

} // namespace k2