'''
MIT License

Copyright (c) 2022 Futurewei Cloud

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

from typing import Tuple
import requests, json
from urllib.parse import urlparse
import copy

class Status:
    "Status returned from HTTP Proxy"

    def __init__(self, result_json):
        "Get status from result json"

        # If seastar catches exception it doesn't populate "status" field
        status = result_json["status"] if "status" in result_json else result_json
        self.code = status["code"]
        self.message = status["message"]

class DBLoc:
    """Indicates a complete location to identify a record.

       It helps doing read/write without spcifying all parameters.

       Example:
          loc = DBLoc("schema1", "collection1", partition_key_name="partitionKey",
             range_key_name="rangeKey", partition_key="pkey1", range_key="rkey1",
             schema_version=1)
          # Write
          txn.write(loc, data1="data 1", data2="data 2")
          # Write to a differnt range key but other parameters same
          txn.write(loc.get_new(range_key="rkey2"), data1="data 3", data2="data 4")
    """

    def __init__(self, schema, coll,
        partition_key_name, range_key_name,
        partition_key=None, range_key=None, schema_version=1):
        self.schema = schema
        self.coll = coll
        self.partition_key_name=partition_key_name
        self.range_key_name = range_key_name
        self.partition_key = partition_key
        self.range_key = range_key
        self.schema_version = schema_version

    def get_new(self, **kwargs):
        "Create a copy and set some attributes"
        obj = copy.copy(self)
        obj.__dict__.update(kwargs)
        return obj

class Txn:
    "Transaction Object"

    def __init__(self, client, txn_id: int):
        self._client = client
        self._txn_id = txn_id

    @property
    def txn_id(self) -> int:
        return self._txn_id

    def _send_req(self, api, request):
        url = self._client.http + api
        r = requests.post(url, data=json.dumps(request))
        result = r.json()
        print(result)
        return result

    def write(self, loc: DBLoc, additional_data={}) -> Status:
        "Write to dbloc"
        record = additional_data.copy()
        record[loc.partition_key_name] = loc.partition_key
        record[loc.range_key_name] = loc.range_key
        request = {"collectionName": loc.coll, "schemaName": loc.schema,
            "txnID" : self._txn_id, "schemaVersion": loc.schema_version,
            "record": record}
        result = self._send_req("/api/Write", request)
        return Status(result)

    def read(self, loc: DBLoc) -> Tuple[Status, object] :
        record = {}
        record[loc.partition_key_name] = loc.partition_key
        record[loc.range_key_name] = loc.range_key
        request = {"collectionName": loc.coll, "schemaName": loc.schema,
            "txnID" : self._txn_id, "record": record}
        result = self._send_req("/api/Read", request)
        return Status(result), result.get("record")

    def end(self, commit=True):
        request = {"txnID": self._txn_id, "commit": commit}
        return Status(self._send_req("/api/EndTxn", request))

class SKVClient:
    "SKV DB client"

    def __init__(self, url: str):
        self.http = url

    def begin_txn(self) -> Tuple[Status, Txn]:
        data = {}
        url = self.http + "/api/BeginTxn"
        r = requests.post(url, data=json.dumps(data))
        result = r.json()
        print(result)
        status = Status(result)
        txn = Txn(self, result.get("txnID"))
        return status, txn
