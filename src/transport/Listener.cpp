//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include "Listener.h"
#include "Log.h"
namespace k2tx
{

Listener::Listener(VirtualFunction::Dist_t& vfs):
    _vfs(vfs) {
    K2DEBUG("");
}

Listener::~Listener()
{
    K2DEBUG("");
}

} // namespace k2tx
