//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#include <memory> // ptr stuff

#include "TCPListener.h"
#include "Log.h"

namespace k2tx {


TCPListener::TCPListener(VirtualFunction::Dist_t& vfs, uint16_t port) : Listener(vfs), _port(port) {
    K2LOG("");
}

TCPListener::TCPListener(const TCPListener &o): Listener(o) {
    K2LOG("");
    this->_port = o._port;
}

TCPListener::TCPListener(TCPListener &&o) : Listener(o) {
    K2LOG("");
    this->_port = o._port;
    o._port = 0;
}
TCPListener::~TCPListener() { K2LOG(""); }

void TCPListener::Start() { K2LOG(""); }

ListenerFactory::BuilderFunc_t TCPListener::Builder(VirtualFunction::Dist_t& vfs, uint16_t port) {
    K2LOG("");
    return [&vfs, port]() mutable -> std::shared_ptr<Listener> {
        K2LOG("");
        return std::static_pointer_cast<Listener>(
            std::make_shared<TCPListener>(vfs, port));
    };
}

seastar::future<> TCPListener::stop()
{
    K2LOG("");
    return seastar::make_ready_future<>();
}
} // namespace k2tx
