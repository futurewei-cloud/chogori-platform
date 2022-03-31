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
        ID = result["txnID"]

        # Write
        record = {"partitionKey": "test1", "rangeKey": "test1", "data": "mydata"}
        request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": ID, "schemaVersion": 1, "record": record}
        url = args.http + "/api/Write"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 201);

        # Read
        record = {"partitionKey": "test1", "rangeKey": "test1"}
        request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": ID, "record": record}
        url = args.http + "/api/Read"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 200);

        # Commit
        request = {"txnID": ID, "commit": True}
        url = args.http + "/api/EndTxn"
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        self.assertEqual(result["status"]["code"], 200);

    # Helper method to populate and send request with default values
    def sendReq(self, api, txn=-1, partitionKeyName="partitionKey", partition="ptest2",
                rangeKeyName="rangeKey", range_key="rtest2",
                dataFieldName="data", data="mydata2",
                coll="HTTPClient", schema="test_schema", ver=1):
        if api ==  "/api/BeginTxn":
            request = {}
        elif api == "/api/Write":
            record = {partitionKeyName: partition, rangeKeyName: range_key,
                      dataFieldName: data}
            request = {"collectionName": coll, "schemaName": schema, "txnID": txn, "schemaVersion": ver, "record": record}
        elif api == "/api/Read":
            record = {partitionKeyName: partition, rangeKeyName: range_key}
            request = {"collectionName": coll, "schemaName": schema, "txnID": txn, "schemaVersion": ver, "record": record}
        elif api == "/api/EndTxn":
            # Commit
            request = {"txnID": txn, "commit": True}
        else:
            raise ValueError('Invalid api' + api)

        url = args.http + api
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(api + ": " + r.text)
        return result

    def test_validation(self):
        # Begin Txn
        result = self.sendReq("/api/BeginTxn")        
        self.assertEqual(result["status"]["code"], 201)
        ID = result["txnID"]

        # Write with bad Txn ID, should fail
        result = self.sendReq("/api/Write", ID+1)
        self.assertEqual(result["status"]["code"], 400)

        # Write with bad collectionName, should fail
        result = self.sendReq("/api/Write", ID, coll="HTTPClient1")
        self.assertEqual(result["status"]["code"], 404)

        # Write with bad schemaName, should fail
        result = self.sendReq("/api/Write", ID, schema="test_schema1")
        self.assertEqual(result["status"]["code"], 404)

        # Write with bad schemaVersion, should fail
        result = self.sendReq("/api/Write", ID, ver=2)
        self.assertEqual(result["status"]["code"], 404)

        # Write with bad partition value data type, should fail
        result = self.sendReq("/api/Write", ID, partition=1)
        self.assertEqual(result["code"], 500)

        # Write with bad range data type, should fail
        result = self.sendReq("/api/Write", ID, range_key=1)
        self.assertEqual(result["code"], 500)

        # Write with bad data field data type, should fail
        result = self.sendReq("/api/Write", ID, data=1)
        self.assertEqual(result["code"], 500)

        # Try read data written by failed requests, should fail
        result = self.sendReq("/api/Read", ID)
        self.assertEqual(result["status"]["code"], 404)

        # Do a valid write, should succeed
        result = self.sendReq("/api/Write", ID)
        self.assertEqual(result["status"]["code"], 201)

        # Read with bad Txn ID, should fail
        result = self.sendReq("/api/Read", ID+1)
        self.assertEqual(result["status"]["code"], 400)

        # Read with bad collectionName, should fail
        result = self.sendReq("/api/Read", ID, coll="HTTPClient1")
        self.assertEqual(result["status"]["code"], 404)

        # Read with bad schemaName, should fail
        result = self.sendReq("/api/Read", ID, schema="test_schema1")
        self.assertEqual(result["status"]["code"], 404)

        # Read with bad partition data type, should fail
        result = self.sendReq("/api/Read", ID, partition=3)
        self.assertEqual(result["code"], 500)

        # Read with bad range data type, should fail
        result = self.sendReq("/api/Read", ID, range_key=3)
        self.assertEqual(result["code"], 500)

        # Do a valid read, should succeed
        result = self.sendReq("/api/Read", ID)
        self.assertEqual(result["status"]["code"], 200)

        # Commit, with bad Txn ID, should fail
        result = self.sendReq("/api/EndTxn", ID+1)
        self.assertEqual(result["status"]["code"], 400)

        # Commit, should succeed
        result = self.sendReq("/api/EndTxn", ID)
        self.assertEqual(result["status"]["code"], 200)

        # Commit again, should fail
        result = self.sendReq("/api/EndTxn", ID)
        self.assertEqual(result["status"]["code"], 400)

    # Test read write conflict between two transactions
    def test_read_write_txn(self):
        # Begin Txn
        result = self.sendReq("/api/BeginTxn")
        self.assertEqual(result["status"]["code"], 201)
        ID = result["txnID"]

        # Write initial data
        result = self.sendReq("/api/Write", ID, partition="ptest3", data="data3")
        self.assertEqual(result["status"]["code"], 201)

        # Commit initial data
        result = self.sendReq("/api/EndTxn", ID)
        self.assertEqual(result["status"]["code"], 200)

        # Begin Txn 1
        result = self.sendReq("/api/BeginTxn")
        self.assertEqual(result["status"]["code"], 201)
        txnId1 = result["txnID"]

        # Begin Txn 2
        result = self.sendReq("/api/BeginTxn")
        self.assertEqual(result["status"]["code"], 201)
        txnId2 = result["txnID"]

        # Read by Txn 2
        result = self.sendReq("/api/Read", txnId2, partition="ptest3")
        self.assertEqual(result["status"]["code"], 200)

        # Update data by Txn 1, should fail with 403: write request cannot be allowed as
        # this key (or key range) has been observed by another transaction.
        result = self.sendReq("/api/Write", txnId1, partition="ptest3", data="data4")
        self.assertEqual(result["status"]["code"], 403)

        # Commit Txn 1, same error as write request
        result = self.sendReq("/api/EndTxn", txnId1)
        self.assertEqual(result["status"]["code"], 403)

        # Commit Txn 2
        result = self.sendReq("/api/EndTxn", txnId2)
        self.assertEqual(result["status"]["code"], 200)


del sys.argv[1:]
unittest.main()
