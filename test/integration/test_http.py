#!/usr/bin/env python3

'''
MIT License

Copyright (c) 2021 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import argparse, unittest, sys
import requests, json
from urllib.parse import urlparse
from skvclient import SKVClient, Txn, DBLoc

parser = argparse.ArgumentParser()
parser.add_argument("--http", help="HTTP API URL")
args = parser.parse_args()


class TestBasicTxn(unittest.TestCase):
    def test_basicTxn(self):
        # Begin Txn
        data = {}
        url = args.http + "/api/BeginTxn"
        r = requests.post(url, data=json.dumps(data))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 201);
        txnId = result["txnID"]

        # Write
        record = {"partitionKey": "test1", "rangeKey": "test1", "data": "mydata"}
        request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": txnId, "schemaVersion": 1, "record": record}
        url = args.http + "/api/Write"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 201);

        # Read
        record = {"partitionKey": "test1", "rangeKey": "test1"}
        request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": txnId, "record": record}
        url = args.http + "/api/Read"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 200);

        # Commit
        request = {"txnID": txnId, "commit": True}
        url = args.http + "/api/EndTxn"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 200);


    def test_validation(self):
        db = SKVClient(args.http)
        # Create a location object
        loc = DBLoc(partition_key_name="partitionKey", range_key_name="rangeKey",
            partition_key="ptest2", range_key="rtest2",
            schema="test_schema", coll="HTTPClient", schema_version=1)
        additional_data =  { "data" :"data1"}

        # Get a txn
        status, txn = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Write/read with bad collection name, should fail
        bad_loc = loc.get_new(coll="HTTPClient1")
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 404)
        status, record = txn.read(bad_loc)
        self.assertEqual(status.code, 404)

        # Write/Read with bad schemaName, should fail
        bad_loc = loc.get_new(schema="test_schema1")
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 404)
        status, record = txn.read(bad_loc)
        self.assertEqual(status.code, 404)

        # Write with bad schema version, should fail
        bad_loc = loc.get_new(schema_version=2)
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 404)

        # Write/Read with bad partition key data type, should fail
        bad_loc = loc.get_new(partition_key=1)
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 500)
        status, _ = txn.read(bad_loc)
        self.assertEqual(status.code, 500)

        # Write/Read with bad range key data type, should fail
        bad_loc = loc.get_new(range_key=1)
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 500)

        # Write with bad data field data type, should fail
        status = txn.write(loc, {"data": 1})
        self.assertEqual(status.code, 500)

        # Do a valid write/read, should succeed
        status = txn.write(loc, additional_data)
        self.assertEqual(status.code, 201)
        status, record = txn.read(loc)
        self.assertEqual(status.code, 200)
        self.assertEqual(record["data"], "data1")
        self.assertEqual(record["partitionKey"], "ptest2")
        self.assertEqual(record["rangeKey"], "rtest2")

        # End transaction, should succeed
        status = txn.end()
        self.assertEqual(status.code, 200)
        # End transaction again, should fail
        status = txn.end()
        self.assertEqual(status.code, 400)

        # Read write using bad Txn ID, Create a txn object with bad txn Id
        badTxn = Txn(db, 10000)
        status = badTxn.write(loc, additional_data)
        self.assertEqual(status.code, 400)
        status, _ = badTxn.read(loc)
        self.assertEqual(status.code, 400)
        status = badTxn.end()
        self.assertEqual(status.code, 400)


    # Test read write conflict between two transactions
    def test_read_write_txn(self):
        db = SKVClient(args.http)
        # Create a location object
        loc = DBLoc(partition_key_name="partitionKey", range_key_name="rangeKey",
            partition_key="ptest3", range_key="rtest3",
            schema="test_schema", coll="HTTPClient", schema_version=1)
        additional_data =  { "data" :"data3"}

        # Populate initial data, Begin Txn
        status, txn = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Write initial data
        status = txn.write(loc, additional_data)
        self.assertEqual(status.code, 201)

        # Commit initial data
        status = txn.end()
        self.assertEqual(status.code, 200)

        # Begin Txn 1
        status, txn1 = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Begin Txn 2
        status, txn2 = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Read by Txn 2
        status, record = txn2.read(loc)
        self.assertEqual(status.code, 200)
        self.assertEqual(record["data"], "data3")
        self.assertEqual(record["partitionKey"], "ptest3")
        self.assertEqual(record["rangeKey"], "rtest3")

        # Update data by Txn 1, should fail with 403: write request cannot be allowed as
        # this key (or key range) has been observed by another transaction.
        status = txn1.write(loc, additional_data)
        self.assertEqual(status.code, 403)

        # Commit Txn 1, same error as write request
        status = txn1.end()
        self.assertEqual(status.code, 403)

        # Commit Txn 2, should succeed
        status = txn2.end()
        self.assertEqual(status.code, 200)

del sys.argv[1:]
unittest.main()
