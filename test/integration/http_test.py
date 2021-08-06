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

import requests, json
from urllib.parse import urlparse
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--http", help="HTTP API URL")
args = parser.parse_args()

# Begin Txn
data = {}
url = args.http + "/api/BeginTxn"
r = requests.post(url, data=json.dumps(data))
result = r.json()
result = result[0][0]
print(result)
if result["status"] != 201:
    print("Error in BeginTxn")
ID = result["txnID"]


# Write
record = {"partitionKey": "test1", "rangeKey": "test1", "data": "mydata"}
request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": ID, "schemaVersion": 1, "record": record}
url = args.http + "/api/Write"
r = requests.post(url, data=json.dumps(request))
result = r.json()
result = result[0][0]
print(result)
if result["status"]["code"] != 201:
    print("Error in Write")

# Read
record = {"partitionKey": "test1", "rangeKey": "test1"}
request = {"collectionName": "HTTPClient", "schemaName": "test_schema", "txnID": ID, "record": record}
url = args.http + "/api/Read"
r = requests.post(url, data=json.dumps(request))
result = r.json()
result = result[0][0]
print(result)
if result["status"]["code"] != 200:
    print("Error in Read")

# Commit
request = {"txnID": ID, "commit": True}
url = args.http + "/api/EndTxn"
r = requests.post(url, data=json.dumps(request))
result = r.json()
result = result[0][0]
print(result)
if result["status"]["code"] != 200:
    print("Error in commit")
