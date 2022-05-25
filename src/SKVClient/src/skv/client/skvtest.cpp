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
#include <iostream>
#include <string>
#include "SKVClient.h"

using namespace skv::http;
bool basicTxnTest(Client& client) {
    K2LOG_I(log::dto, "Sending begin txn");
    auto result = client.beginTxn(dto::TxnOptions{})
        .then([] (auto&& fut) {
            auto&& [status, txn]  = fut.get();
            K2LOG_I(log::dto, "Got txn handle {}", status);
            K2ASSERT(log::dto, status.is2xxOK(), "Begin txn failed");
            return  std::move(txn);
        })
        .then([] (auto&& fut) {
            TxnHandle txn  = fut.get();
            return txn.endTxn(true);
        })
        .then([] (auto&& fut) {
            auto&& [status] = fut.get().get();
            K2ASSERT(log::dto, status.is2xxOK(), "End txn failed");
            K2LOG_I(log::dto, "End txn ends {}", status);

            return true;
        }).get();
    return result;
}

int main() {
    skv::http::Client client;
    auto result = basicTxnTest(client);
    return result ? 0 : 1;
}
