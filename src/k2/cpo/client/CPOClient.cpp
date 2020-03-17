//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include "CPOClient.h"

namespace k2 {

CPOClient::CPOClient(String cpo_url) {
    cpo = RPC().getTXEndpoint(cpo_url);
}

void CPOClient::FulfillWaiters(const String& name, const Status& status) {
    auto& waiters = requestWaiters[name];

    for (auto it = waiters.begin(); it != waiters.end(); ++it) {
        it->set_value(status);
    }

    requestWaiters.erase(name);
}

} // ns k2
