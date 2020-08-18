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

#include <k2/appbase/Appbase.h>
#include <k2/common/Common.h>
#include <k2/persistence/plog/PlogClient.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sleep.hh>
#include <k2/common/Chrono.h>
#include <k2/config/Config.h>
#include <k2/dto/Collection.h>
#include <k2/dto/Persistence.h>
#include <k2/transport/RPCDispatcher.h>
#include <k2/transport/RPCTypes.h>
#include <k2/transport/Status.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <cstdlib>
#include <cctype>
#include <algorithm>
namespace k2 {
// implements an example RPC Service which can send/receive messages

class PLOGService {
public:  // application lifespan


    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        return seastar::make_ready_future<>();
    }

    // called after construction
    void start() {
        (void)seastar::sleep(1s)
        .then([&] (){
            return client.GetPlogServerUrls();
        })
        .then([&] (){
            return client.create();
        })
        .then([&](auto&& resp) {
            auto& [status, plogId] = resp;
            Payload payload([] { return Binary(4096); });
            K2INFO(payload.getSize());
            payload.write("1234567890");
            K2INFO(payload.getSize());
            return client.append(std::move(plogId), 100, std::move(payload));
        })
        .then([&] (auto&& resp){
            auto& [status, offset] = resp;
            K2INFO("New Offest is " << offset);
        });
    }

private:
    PlogClient client;
};

}

int main(int argc, char** argv) {
    k2::App app("RPCEchoDemo");
    app.addApplet<k2::PLOGService>();
    app.addOptions()
        ("cpo_url", bpo::value<k2::String>()->default_value(""), "The URL of the CPO");
    return app.start(argc, argv);
}
