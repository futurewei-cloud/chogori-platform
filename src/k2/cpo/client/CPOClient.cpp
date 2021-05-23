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

#include "CPOClient.h"

#include <k2/dto/FieldTypes.h>

namespace k2 {

CPOClient::CPOClient() {
    K2LOG_D(log::cpoclient, "ctor");
}

void CPOClient::init(String cpo_url) {
    cpo = RPC().getTXEndpoint(cpo_url);
    K2ASSERT(log::cpoclient, cpo, "unable to get endpoint for url {}", cpo_url);
}

CPOClient::~CPOClient() {
    K2LOG_D(log::cpoclient, "dtor");
}

void CPOClient::_fulfillWaiters(const String& name, const Status& status) {
    auto& waiters = requestWaiters[name];

    for (auto it = waiters.begin(); it != waiters.end(); ++it) {
        it->set_value(status);
    }

    requestWaiters.erase(name);
}

seastar::future<k2::Status> CPOClient::createSchema(const String& collectionName, k2::dto::Schema schema) {
    k2::dto::CreateSchemaRequest request{ collectionName, std::move(schema) };
    return k2::RPC().callRPC<k2::dto::CreateSchemaRequest, k2::dto::CreateSchemaResponse>(k2::dto::Verbs::CPO_SCHEMA_CREATE, request, *cpo, schema_request_timeout())
    .then([] (auto&& response) {
        auto& [status, r] = response;
        return status;
    });
}

seastar::future<std::tuple<k2::Status, std::vector<k2::dto::Schema>>> CPOClient::getSchemas(const String& collectionName) {
    k2::dto::GetSchemasRequest request { collectionName };
    return k2::RPC().callRPC<k2::dto::GetSchemasRequest, k2::dto::GetSchemasResponse>(k2::dto::Verbs::CPO_SCHEMAS_GET, request, *cpo, schema_request_timeout())
    .then([] (auto && response) {
        auto& [status, r] = response;
        return std::tuple(std::move(status), std::move(r.schemas));
    });
}

} // ns k2
