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
#include "Common.h"
#include "Status.h"
#include "SKVRecord.h"

#include "httplib/httplib.h"

namespace k2 {

class HTTPMessageClient {
private:
    httplib::Client client;

    enum class Method:uint8_t {
        POST
    };
    struct Binary {
        typedef std::function<void(char*)> Deleter;
        Deleter deleteDeleter = [] (char* d) { delete d;}

        Binary(): Binary(0,0, deleteDeleter) {}

        template <typename DeleterFunc>
        Binary(DeleterFunc&& deleter): Binary(0,0, std::move(deleter)) {}

        template<typename DeleterFunc>
        Binary(char* data, size_t size, DeleterFunc&& d) : data(data), size(size), _deleter(std::move(d)) {}
        Binary(Binary&& o) {
            data(o.data);
            size(o.size);
            _deleter=std::move(o._deleter);
            o.data=0;
            o.size=0;
        }
        Binary(const Binary& o) = delete;
        Binary& operator=(const Binary& o) = delete;
        Binary& operator=(Binary&& o) {
            data(o.data);
            size(o.size);
            _deleter = std::move(o._deleter);
            o.data = 0;
            o.size = 0;
            return *this;
        }
        ~Binary() {
            _deleter(data);
            data = 0;
            size = 0;
        }
        char* data;
        size_t size;
    private:
        Deleter _deleter;
    };
public:
    HTTPMessageClient(std::string server="localhost", int port=30000) {
        client = httplib::Client(server, port);
    }

    // send a single HTTP message and return the status and expected response object
    template <typename RequestT, typename ResponseT>
    boost::future<Response<ResponseT>> POST(String path, RequestT&& obj) {
        return _makeCall<RequestT, ResponseT>(Method::POST, path, obj);
    }

private:
    // helper method. Make the given http call, to the given path with the given request object.
    template <typename RequestT, typename ResponseT>
    boost::future<Response<ResponseT>> _makeCall(Method method, String path, RequestT&& obj) {
        auto&& [status, buf] = _serialize(obj);
        if (!status.is2xxOK()) {
            return MakeResponse(std::move(status), ResponseT{});
        }
        return _doSend(method, path, std::move(buf))
            .then([] (auto&& fut) {
                auto &&[status, buf] = fut.get();
                if (!status.is2xxOK()) {
                    return MakeResponse(std::move(status), ResponseT{});
                }
                struct ExpResponse {
                    ResponseT obj;
                    Status status;
                };
                auto&& [desStatus, resp] = _deserialize<ExpResponse>(buf);
                if (!desStatus.is2xxOK()) {
                    return MakeResponse(std::move(desStatus), std::move(obj));
                }

                return MakeResponse(std::move(resp.status), std::move(resp.obj));
            });
    }

    boost::future<Response<Binary>> _doSend(Method method, String path, Binary&& request) {
        // send the payload via http
        httplib::Result result;
        switch (method) {
            case Method::POST:
                result = client.Post(path, request.data, request.size, "application/x-msgpack");
                break;
            default:
                throw std::runtime_error("Unknown method for HTTPMessageClient _doSend");
        }

        Status responseStatus{.code=result->status, .message=""};
        char* bodyData = result->body.data();
        size_t bodySize = result->body.size();
        Binary responseBody(bodyData, bodySize, [std::move(result->body)] (char*) {});
        return make_ready_future(std::move(responseStatus), std::move(responseBody));
    }

    template<typename T>
    Response<Binary> _serialize(T& obj) {
        Binary buf([](char* ptr) { MPACK_FREE(ptr);});
        mpack_writer_t writer;
        mpack_writer_init_growable(&writer, &buf.data, &buf.size);

        // write the object
        obj.k2PackTo(&writer);

        // destroying the writer detects errors and sets the data pointer.
        // data will be NULL if there is an error - we aren't responsible for free-ing it in this case
        if (mpack_writer_destroy(&writer) != mpack_ok) {
            return MakeResponse(Statuses::S400_Bad_Request("Unable to serialize object"), std::move(buf));
        }
        return MakeResponse(Statuses::S200_OK(), std::move(buf));
    }

    template<typename T>
    Response<T> _deserialize(Binary& buf) {
        mpack_tree_t tree;
        mpack_tree_init_data(&tree, buf.data, buf.size);
        mpack_tree_parse(&tree);
        mpack_node_t root = mpack_tree_root(&tree);

        T obj;
        if (!obj.k2UnpackFrom(root)) {
            return MakeResponse(Statuses::S500_Internal_Server_Error("Unable to parse response from server"), T{});
        }

        // clean up and check for errors
        if (mpack_tree_destroy(&tree) != mpack_ok) {
            return MakeResponse(Statuses::S400_Bad_Request("Unable to deserialize buffer"), T{});
        }
        return MakeResponse(Statuses::S200_OK(), std::move(obj));
    }
 };

class TxnHandle {
public:
    TxnHandle(TxnId id) {}
    TxnHandle() {}
    boost::future<Response<EndTxnResponse>> endTxn(EndTxnRequest request) {
        _client.POST<EndTxnRequest, EndTxnResponse>("/api/v1/endTxn", std::move(request));
    }
    boost::future<Response<ReadResponse>> read(ReadRequest request);
    boost::future<Response<WriteResponse>> write(WriteRequest request);
    boost::future<Response<UpdateResponse>> update(UpdateRequest request);
    boost::future<Response<CreateQueryResponse>> createQuery(CreateQueryRequest request);
    boost::future<Response<QueryResponse>> query(QueryRequest request);

private:
    HTTPMessageClient& _client;
};

class SKVClient {
public:
    SKVClient(){}
    ~SKVClient() {}
    boost::future<Response<CreateSchemaResponse>> createSchema(CreateSchemaRequest request);
    boost::future<Response<GetSchemaResponse>> getSchema(GetSchemaRequest request);
    boost::future<Response<CreateCollectionResponse>> createCollection(CreateCollectionRequest request);
    boost::future<Response<GetCollectionResponse>> getCollection(GetCollectionRequest request);
    boost::future<Response<TxnHandle>> beginTxn(BeginTxnRequest request);
private:
    HTTPMessageClient _client;
};

} // ns k2
