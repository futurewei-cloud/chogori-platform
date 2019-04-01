//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <seastar/core/reactor.hh>

#include "VirtualFunction.h"
#include "Log.h"
namespace k2tx {

VirtualFunction::~VirtualFunction() { K2DEBUG(""); }

unsigned VirtualFunction::GetVFID() const {
    // TODO for now just return the cpu ID
    K2DEBUG("");
    return seastar::engine().cpu_id();
}

void VirtualFunction::Start(){
    K2DEBUG("");
    // TODO here we add logic which should be executed after the system has been
    // bootstrapped
}

seastar::future<> VirtualFunction::stop()
{
    K2DEBUG("");
    return seastar::make_ready_future<>();
}

} // namespace
