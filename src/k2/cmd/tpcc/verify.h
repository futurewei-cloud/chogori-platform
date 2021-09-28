/*
MIT License

Copyright(c) 2021 Futurewei Cloud

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

#include <cmath>

#include <k2/appbase/Appbase.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/module/k23si/client/k23si_client.h>

#include "schema.h"
#include "transactions.h"
#include "Log.h"

using namespace seastar;
using namespace k2;

class AtomicVerify
{
public:
    AtomicVerify(RandomContext& random, K23SIClient& client, int16_t max_w_id) :
            _random(random), _client(client), _payment(PaymentT(random, client, 1, max_w_id)),
            _payment2(PaymentT(random, client, 1, max_w_id)), _max_w_id(max_w_id) {}

private:
    struct ValuesToCompare {
        std::decimal::decimal64 w_ytd;
        std::decimal::decimal64 d_ytd;
        std::decimal::decimal64 c_ytd;
        std::decimal::decimal64 c_balance;
        uint16_t c_payments;
    };

    future<> getVerificationValues(ValuesToCompare& values);
    future<> runPaymentTxn(PaymentT& _payment);
    void compareCommitValues();
    void compareAbortValues();

    RandomContext& _random;
    K23SIClient& _client;
    PaymentT _payment;
    PaymentT _payment2;
    K2TxnHandle _txn;
    ValuesToCompare _before;
    ValuesToCompare _after;
    int16_t _max_w_id;

public:
    future<> run();
};

class ConsistencyVerify
{
public:
    ConsistencyVerify(K23SIClient& client, int16_t max_w_id) :
            _client(client), _max_w_id(max_w_id) {}

    future<> run();
private:
    using consistencyOp = future<> (ConsistencyVerify::*)();

    future<> verifyWarehouseYTD();
    future<> verifyOrderIDs();
    future<> verifyNewOrderIDs();
    future<> verifyOrderLineCount();
    future<> verifyCarrierID();
    future<int16_t> countOrderLineRows(int64_t oid);
    future<> verifyOrderLineByOrder();
    future<> runForEachWarehouse(consistencyOp op);
    future<> runForEachWarehouseDistrict(consistencyOp op);

    K23SIClient& _client;
    K2TxnHandle _txn;
    Query _query;
    int16_t _max_w_id;
    int16_t _cur_w_id;
    int16_t _cur_d_id;
    int64_t _nextOrderID;

    ConfigVar<int16_t> _districts_per_warehouse{"districts_per_warehouse"};
};

