//<!--
//    (C)opyright Futurewei Technologies Inc, 2019
//-->
#pragma once
#include <seastar/core/distributed.hh>

#include "Listener.h"
#include "ListenerFactory.h"

namespace k2tx {

class TCPListener: public Listener {
public:
    static ListenerFactory::BuilderFunc_t Builder(VirtualFunction::Dist_t& vfs, uint16_t port);
public:
    TCPListener(VirtualFunction::Dist_t& vfs, uint16_t port);
    TCPListener(const TCPListener& o);
    TCPListener(TCPListener&& o);
    TCPListener &operator=(const TCPListener& o) = default;
    TCPListener &operator=(TCPListener&& o) = default;
    virtual ~TCPListener();

    seastar::future<> stop() override;

    void Start() override;

private:
    uint16_t _port;
}; // class TCPListener

} // namespace k2tx
