//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "Listener.h"
#include "Log.h"
namespace k2tx
{

Listener::Listener(VirtualFunction::Dist_t& vfs):
    _vfs(vfs) {
    K2LOG("");
}

Listener::~Listener()
{
    K2LOG("");
}

} // namespace k2tx
