/*
MIT License

Copyright(c) 2022 Futurewei Cloud

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

#pragma once
#include <skvhttp/common/Common.h>
#include <skvhttp/common/Status.h>
#include <skvhttp/dto/Collection.h>
#include <skvhttp/dto/ControlPlaneOracle.h>
#include <skvhttp/dto/K23SI.h>
#include <skvhttp/dto/SKVRecord.h>
#include <skvhttp/httplib/httplib.h>

namespace skv::http {

class HTTPMessageClient {
private:
    httplib::Client client;

    enum class Method : uint8_t {
        POST
    };

public:
    HTTPMessageClient(std::string server = "localhost", int port = 30000): client(server, port) {}

    // send a single HTTP message and return the status and expected response object
    template <typename RequestT, typename ResponseT>
    boost::future<Response<ResponseT>> POST(String path, RequestT&& obj) {
        return _makeCall<RequestT, ResponseT>(Method::POST, std::move(path), std::move(obj));
    }

private:
    // helper method. Make the given http call, to the given path with the given request object.
    template <typename RequestT, typename ResponseT>
    boost::future<Response<ResponseT>> _makeCall(Method method, String path, RequestT&& reqObj) {
        auto&& [status, buf] = _serialize(reqObj);
        if (!status.is2xxOK()) {
            return MakeResponse(std::move(status), ResponseT{});
        }
        return _doSend(method, path, std::move(buf))
            .then([this](auto&& fut) {
                auto&& [status, buf] = fut.get();
                if (!status.is2xxOK()) {
                    return Response<ResponseT>(std::move(status), ResponseT{});
                }
                auto&& [desStatus, resp] = _deserialize<Response<ResponseT>>(buf);
                if (!desStatus.is2xxOK()) {
                    return Response<ResponseT>(std::move(desStatus), ResponseT{});
                }
                return std::move(resp);
            });
    }
    Response<Binary> _processResponse(httplib::Result&& result) {
        Status responseStatus{.code = result->status, .message = result->reason};
        char* bodyData = result->body.data();
        size_t bodySize = result->body.size();
        Binary responseBody(bodyData, bodySize, [str=std::move(result->body)]() {});
        return {std::move(responseStatus), std::move(responseBody)};
    }

    boost::future<Response<Binary>> _doSend(Method method, String path, Binary&& request) {
        // send the payload via http
        switch (method) {
            case Method::POST:
                return make_ready_future(_processResponse(client.Post(path.c_str(), request.data(), request.size(), "application/x-msgpack")));
            default:
                throw std::runtime_error("Unknown method for HTTPMessageClient _doSend");
        }
    }

    template <typename T>
    Response<Binary> _serialize(T& obj) {
        MPackWriter writer;
        writer.write(obj);
        Binary buf;
        if (!writer.flush(buf)) {
            return {Statuses::S400_Bad_Request("Unable to serialize object"), std::move(buf)};
        }
        return {Statuses::S200_OK, std::move(buf)};
    }

    template <typename T>
    Response<T> _deserialize(Binary& buf) {
        MPackReader reader(buf);
        T obj;
        if (!reader.read(obj)) {
            return {Statuses::S400_Bad_Request("Unable to deserialize buffer"), T{}};
        }
        return {Statuses::S200_OK, std::move(obj)};
    }
};

class TxnHandle {
public:
    TxnHandle(HTTPMessageClient* client, dto::Timestamp id):_client(client), _id(id) {}
    boost::future<Response<>> endTxn(bool doCommit);
    boost::future<Response<dto::SKVRecord>> read(dto::SKVRecord record);
    boost::future<Response<dto::K23SIWriteResponse>> write(dto::SKVRecord& record, bool erase=false,
                                       dto::ExistencePrecondition precondition=dto::ExistencePrecondition::None);
    boost::future<Response<>> partialUpdate(dto::SKVRecord& record, std::vector<String> fieldNamesForUpdate);

    boost::future<Response<std::vector<dto::SKVRecord>>> query(dto::Query& query);
    boost::future<Response<dto::Query>> createQuery(const String& collectionName, const String& schemaName);

private:
    HTTPMessageClient* _client;
    dto::Timestamp _id;
};

class Client {
public:
    Client() {}
    ~Client() {}
    boost::future<Response<>> createSchema(const String& collectionName, const dto::Schema& schema);
    boost::future<Response<dto::Schema>> getSchema(const String& collectionName, const String& schemaName, int64_t schemaVersion=dto::ANY_SCHEMA_VERSION);
    boost::future<Response<>> createCollection(dto::CollectionMetadata metadata, std::vector<String> rangeEnds);
    boost::future<Response<dto::Collection>> getCollection(const String& collectionName);
    boost::future<Response<TxnHandle>> beginTxn(dto::TxnOptions options);

private:
    HTTPMessageClient _client;
};

}  // namespace skv::http
