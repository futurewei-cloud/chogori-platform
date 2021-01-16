/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#include <seastar/core/future.hh>

#include "RPCProtocolFactory.h"
#include "Log.h"

namespace k2 {
RPCProtocolFactory::RPCProtocolFactory(BuilderFunc_t builder): _builder(builder) {
    K2LOG_D(log::tx, "ctor");
}

RPCProtocolFactory::~RPCProtocolFactory() {
    K2LOG_D(log::tx, "dtor");
}

void RPCProtocolFactory::start() {
    K2LOG_D(log::tx, "start");
    // Create the protocol instance
    _instance = _builder();
    if (_instance) {
        _instance->start();
    }
}

seastar::future<> RPCProtocolFactory::stop() {
    K2LOG_D(log::tx, "stop");
    if (_instance) {
        // pass-on the signal to stop
        auto result = _instance->stop();
        _instance = nullptr;
        return result;
    }
    return seastar::make_ready_future<>();
}

seastar::shared_ptr<IRPCProtocol> RPCProtocolFactory::instance() { return _instance; }

} // k2
