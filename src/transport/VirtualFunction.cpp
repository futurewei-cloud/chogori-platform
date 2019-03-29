//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <seastar/core/reactor.hh>

#include "VirtualFunction.h"
#include "Log.h"
namespace k2tx {

VirtualFunction::~VirtualFunction() { K2LOG(""); }

unsigned VirtualFunction::GetVFID() const {
    // TODO for now just return the cpu ID
    K2LOG("");
    return seastar::engine().cpu_id();
}

void VirtualFunction::Start(){
    K2LOG("");
    // TODO here we add logic which should be executed after the system has been
    // bootstrapped
}

seastar::future<> VirtualFunction::stop()
{
    K2LOG("");
    return seastar::make_ready_future<>();
}

} // namespace
