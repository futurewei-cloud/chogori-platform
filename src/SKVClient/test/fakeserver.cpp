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

// Code to test http serialization/deserialization from client

#include <iostream>
#include <string>
#include <boost/program_options.hpp>
#include <skv/client/SKVClient.h>

using namespace skv::http;

const char *response_type = "application/x-msgpack";

template <typename T>
    Binary _serialize(T&& obj) {
    MPackWriter writer;
    Response<T> resp(Statuses::S200_OK, std::forward<T>(obj));
    writer.write(resp);
    Binary buf;
    if (!writer.flush(buf)) {
        throw std::runtime_error("Serialization failed");
    }
    return buf;
}

void setError(httplib::Response &resp, const Status& status) {
    resp.status = status.code;
    resp.set_content(status.message, "text/plain");
}

template <typename T>
bool _sendResponse(httplib::Response &resp, T& obj) {
    try {
        auto&& buf = _serialize(obj);
        resp.status = 200;
        resp.set_content(buf.data(), buf.size(), response_type);
        return true;
    } catch(std::runtime_error& e) {
        setError(resp, Statuses::S500_Internal_Server_Error);
    }
    return false;
 }

template <typename T>
T _deserialize(const std::string& body) {
    String copy(body);
    Binary buf(std::move(copy));
    MPackReader reader(buf);
    T obj;
    if (!reader.read(obj)) {
        throw dto::DeserializationError();
    }
    return obj;
}

template<typename ReqT, typename RespT>
void process(httplib::Server& svr, const char *path, std::function<RespT(ReqT&&)> fn) {
    svr.Post(path, [fn](const httplib::Request &req, httplib::Response &resp) {
        ReqT reqObj = _deserialize<ReqT>(req.body);
        K2LOG_I(log::dto, "Got {} request {}", req.path, reqObj);
        RespT respObj = fn(std::move(reqObj));
        bool success = _sendResponse(resp, respObj);
        K2LOG_I(log::dto, "Sent  {} {}", respObj, success);
    });
}

namespace po = boost::program_options;
po::options_description desc("Allowed options");
po::variables_map vm;


int main(int argc, char **argv) {
    desc.add_options()
    ("help", "produce help message")
    ("port", po::value<int>()->default_value(30000), "server port");

    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
        std::cout << desc << "\n";
        return 0;
    }

    // HTTP
    httplib::Server svr;
    uint64_t txnId = 0;

    process<dto::K23SIBeginTxnRequest, dto::K23SIBeginTxnResponse>(svr,
        "/api/v1/beginTxn", [&txnId] (auto&&) {
        return  dto::K23SIBeginTxnResponse{dto::Timestamp(100000, 1, txnId++)};
    });

    process<dto::K23SITxnEndRequest, dto::K23SITxnEndResponse>(svr,
        "/api/v1/endTxn", [] (auto&&) {
        return  dto::K23SITxnEndResponse{};
    });

    svr.listen("0.0.0.0", vm["port"].as<int>());
}

