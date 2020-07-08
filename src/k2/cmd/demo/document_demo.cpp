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
#include <k2/appbase/AppEssentials.h>
#include <k2/dto/ControlPlaneOracle.h>
#include <k2/dto/MessageVerbs.h>
#include <k2/module/k23si/client/k23si_client.h>
#include <k2/tso/client_lib/tso_clientlib.h>

using namespace k2;

class Demo {
public:  // application lifespan
    Demo():
        _client(K23SIClient(K23SIClientConfig())),
        _stopped(true)
    {
        K2INFO("ctor");
    };

    ~Demo() {
        K2INFO("dtor");
    }

    // required for seastar::distributed interface
    seastar::future<> gracefulStop() {
        K2INFO("stop");
        if (_stopped) {
            return seastar::make_ready_future<>();
        }
        _stopped = true;
        // unregister all observers
        k2::RPC().registerLowTransportMemoryObserver(nullptr);

        return _stopPromise.get_future();
    }

    seastar::future<> start() {
        _stopped = false;
        k2::RPC().registerLowTransportMemoryObserver([](const k2::String& ttype, size_t requiredReleaseBytes) {
            K2WARN("We're low on memory in transport: " << ttype <<", requires release of "<< requiredReleaseBytes << " bytes");
        });

        return _client.start().then([this] () { return _demo(); })
        .handle_exception([this](auto exc) {
            K2ERROR_EXC("Unable to execute benchmark", exc);
            _stopped = true;
            return seastar::make_ready_future<>();
        }).finally([this]() {
            K2INFO("Done with benchmark");
            _stopped = true;
            _stopPromise.set_value();
            return seastar::make_ready_future<>();
        });
    }

private:
    seastar::future<> _demo() {
        return _client.makeCollection("demo").discard_result()
        .then([this] () {
            dto::Schema schema;
            schema.name = "demo_schema";
            schema.version = 1; // We expect to create version 1 of this schema
            schema.fields = std::vector<dto::DocumentFieldType> {
                    dto::DocumentFieldType::STRING,
                    dto::DocumentFieldType::STRING,
                    dto::DocumentFieldType::UINT32T};
            schema.fieldNames = std::vector<String>{"LastName", "FirstName", "Balance"};
            // LastName is our partition key
            schema.partitionKeyFields = std::vector<uint32_t>{0};
            // FirstName is our range key
            schema.rangeKeyFields = std::vector<uint32_t>{1};

            dto::CreateSchemaRequest request{ "demo_schema", std::move(schema) };
            return RPC().callRPC<dto::CreateSchemaRequest, dto::CreateSchemaResponse>(dto::Verbs::CPO_SCHEMA_CREATE, request, *(RPC().getTXEndpoint(_cpo())), 1s);
        })
        .then([this] (auto&& response) {
            auto& [status, k2response] = response;
            K2ASSERT(status.is2xxOK(), "Create schema failed");

            return _client.beginTxn(K2TxnOptions{});
        })
        .then([this] (K2TxnHandle&& txn) {
            SerializableDocument writeDoc = _client.makeSerializableDocument("demo", "demo_schema");
            writeDoc.serializeField<String, dto::DocumentFieldType::STRING>("Baggins", "LastName");
            writeDoc.serializeField<String, dto::DocumentFieldType::STRING>("Bilbo", "FirstName");
            writeDoc.serializeField<uint32_t, dto::DocumentFieldType::UINT32T>(777, "Balance");

            return txn.write(std::move(writeDoc))
            .then([&txn] (WriteResult&& result) {
                K2ASSERT(result.status.is2xxOK(), "Write failed");
                return std::move(txn);
            });
        })
        .then([this] (K2TxnHandle&& txn) {
            SerializableDocument readDoc = _client.makeSerializableDocument("demo", "demo_schema");
            // We need the partition and range key fields to do the read request
            readDoc.serializeField<String, dto::DocumentFieldType::STRING>("Baggins", "LastName");
            readDoc.serializeField<String, dto::DocumentFieldType::STRING>("Bilbo", "FirstName");

            return txn.read(std::move(readDoc))
            .then([&txn] (ReadResult<SerializableDocument>&& result) {
                K2ASSERT(result.status.is2xxOK(), "Read failed");

                uint32_t balance = result.getValue().deserializeField<uint32_t, dto::DocumentFieldType::UINT32T>("Balance");
                K2ASSERT(balance == 777, "We did not read our write");
                return std::move(txn);
            });
        })
        .then([] (K2TxnHandle&& txn) {
            return txn.end(true);
        })
        .then([] (auto&& result) {
            (void) result;
            return seastar::make_ready_future<>();
        });
    }

private:
    K23SIClient _client;
    bool _stopped;
    seastar::promise<> _stopPromise;
    ConfigVar<std::vector<String>> _tcpRemotes{"tcp_remotes"};
    ConfigVar<String> _cpo{"cpo"};
}; // class Demo

int main(int argc, char** argv) {;
    k2::App app("DocumentDemo");
    app.addOptions()
        ("tcp_remotes", bpo::value<std::vector<k2::String>>()->multitoken()->default_value(std::vector<k2::String>()), "A list(space-delimited) of TCP remote endpoints to assign to each core. e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("cpo", bpo::value<k2::String>(), "URL of Control Plane Oracle (CPO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("tso_endpoint", bpo::value<k2::String>(), "URL of Timestamp Oracle (TSO), e.g. 'tcp+k2rpc://192.168.1.2:12345'")
        ("partition_request_timeout", bpo::value<ParseableDuration>(), "Timeout of K23SI operations, as chrono literals")
        ("cpo_request_timeout", bpo::value<ParseableDuration>(), "CPO request timeout")
        ("cpo_request_backoff", bpo::value<ParseableDuration>(), "CPO request backoff");

    app.addApplet<k2::TSO_ClientLib>(0s);
    app.addApplet<Demo>();
    return app.start(argc, argv);
}
