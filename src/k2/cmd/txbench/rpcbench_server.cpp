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

// stl
#include <unordered_map>

#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>

#include "rpcbench_common.h"
#include "Log.h"
using namespace k2;

class Service {
public:  // application lifespan
    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2LOG_I(log::txbench, "stop");
        return seastar::make_ready_future();
    }

    seastar::future<> start() {
        RPC().registerRPCObserver<TXBenchStartSession, TXBenchStartSessionAck>
        (MsgVerbs::START_SESSION, [this](TXBenchStartSession&& request) {
            _sessionId++;
            BenchSession session(_sessionId, request.responseSize);
            _sessions.insert_or_assign(_sessionId, std::move(session));
            K2LOG_I(log::txbench, "Starting new session: {}", _sessionId);
            return RPCResponse(Statuses::S200_OK("started"), TXBenchStartSessionAck{.sessionId=_sessionId});
        });

        RPC().registerRPCObserver<TXBenchRequest<Payload>, TXBenchResponse<Payload>>
        (MsgVerbs::REQUEST, [this](TXBenchRequest<Payload>&& request) {
            TXBenchResponse<Payload> response;
            response.sessionId = request.sessionId;

            auto siditer = _sessions.find(request.sessionId);
            if (siditer == _sessions.end()) {
                return RPCResponse(Statuses::S404_Not_Found("session not found"), std::move(response));
            }

            auto& session = siditer->second;
            response.data.val = session.dataShare.shareAll();
            return RPCResponse(Statuses::S200_OK("received"), std::move(response));
        });

        RPC().registerRPCObserver<TXBenchRequest<Payload>, TXBenchResponse<String>>
        (MsgVerbs::REQUEST_COPY, [this](TXBenchRequest<Payload>&& request) {
            TXBenchResponse<String> response;
            response.sessionId = request.sessionId;

            auto siditer = _sessions.find(request.sessionId);
            if (siditer == _sessions.end()) {
                return RPCResponse(Statuses::S404_Not_Found("session not found"), std::move(response));
            }

            auto& session = siditer->second;
            response.data.val = session.dataCopy;
            return RPCResponse(Statuses::S200_OK("received"), std::move(response));
        });
        return seastar::make_ready_future();
    }

private:
    std::unordered_map<uint64_t, BenchSession> _sessions;
    uint64_t _sessionId = seastar::engine().cpu_id();
}; // class Service

int main(int argc, char** argv) {
    App app("RPCBenchService");
    app.addApplet<Service>();
    return app.start(argc, argv);
}
