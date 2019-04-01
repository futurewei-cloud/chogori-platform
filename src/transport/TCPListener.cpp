//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <memory> // ptr stuff

#include "TCPListener.h"
#include "Log.h"

namespace k2tx {


TCPListener::TCPListener(VirtualFunction::Dist_t& vfs, uint16_t port) : Listener(vfs), _port(port) {
    K2DEBUG("");
}

TCPListener::TCPListener(const TCPListener &o): Listener(o) {
    K2DEBUG("");
    this->_port = o._port;
}

TCPListener::TCPListener(TCPListener &&o) : Listener(o) {
    K2DEBUG("");
    this->_port = o._port;
    o._port = 0;
}
TCPListener::~TCPListener() { K2DEBUG(""); }

void TCPListener::Start() { K2DEBUG(""); }

ListenerFactory::BuilderFunc_t TCPListener::Builder(VirtualFunction::Dist_t& vfs, uint16_t port) {
    K2DEBUG("");
    return [&vfs, port]() mutable -> std::shared_ptr<Listener> {
        K2DEBUG("");
        return std::static_pointer_cast<Listener>(
            std::make_shared<TCPListener>(vfs, port));
    };
}

seastar::future<> TCPListener::stop()
{
    K2DEBUG("");
    return seastar::make_ready_future<>();
}
} // namespace k2tx
