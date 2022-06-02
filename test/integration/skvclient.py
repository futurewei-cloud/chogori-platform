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

import requests
from urllib.parse import urlparse
import copy
from enum import Enum
from typing import List
import re
import msgpack
from datetime import timedelta

def serializeTD(tdelta):
    return int(tdelta.total_seconds()*1000*1000*1000)
class Status:
    "Status returned from HTTP Proxy"

    def __init__(self, status):
        "Get status from status tuple"
        self.code, self.message = status
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

    # Get a data record from partition and range
    # allowing multiple partitions or ranges
    def get_fields(self) -> dict:
        record = {}
        if isinstance(self.partition_key_name, list):
            for n, v in zip(self.partition_key_name, self.partition_key):
                record[n] = v
        else:
            record[self.partition_key_name] = self.partition_key

        if isinstance(self.range_key_name, list):
            for n, v in zip(self.range_key_name, self.range_key):
                record[n] = v
        else:
            record[self.range_key_name] = self.range_key

        return record


class Query:
    def __init__(self, query_id: int):
        self.query_id = query_id
        self.done = False

ListOfDict = List[dict]

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
        return result

    def write(self, loc: DBLoc, additional_data={}) -> Status:
        "Write to dbloc"
        record = additional_data.copy()
        record.update(loc.get_fields())
        request = {"collectionName": loc.coll, "schemaName": loc.schema,
            "txnID" : self._txn_id, "schemaVersion": loc.schema_version,
            "record": record}
        result = self._send_req("/api/Write", request)
        return Status(result)

    def read(self, loc: DBLoc) :
        record = loc.get_fields()
        request = {"collectionName": loc.coll, "schemaName": loc.schema,
            "txnID" : self._txn_id, "record": record}
        result = self._send_req("/api/Read", request)
        output = result["response"].get("record") if "response" in result else None
        return Status(result), output

    def query(self, query: Query):
        request = {"txnID" : self._txn_id, "queryID": query.query_id}
        result = self._send_req("/api/Query", request)
        recores:dict = []
        if "response" in result:
            query.done = result["response"]["done"]
            records = result["response"]["records"]
        return Status(result), records

    def queryAll(self, query: Query):
        records: [dict] = []
        while not query.done:
            status, r = self.query(query)
            if status.code != 200:
                return status, records
            records += r

        return status, records

    def end(self, commit=True):
        request = {"txnID": self._txn_id, "commit": commit}
        return Status(self._send_req("/api/EndTxn", request))

class FieldType(int, Enum):
    NULL_T:     int     = 0
    STRING:     int     = 1
    INT32T:     int     = 2
    INT64T:     int     = 3
    FLOAT:      int     = 4
    DOUBLE:     int     = 5
    BOOL:       int     = 6
    DECIMAL64:  int     = 7
    DECIMAL168: int     = 8

    def serialize(self):
        return self.value

class SchemaField:
    def __init__(self, type: FieldType, name: str,
        descending: bool= False, nullLast: bool = False):
        self.type = type
        self.name = name
        self.descending = descending
        self.nullLast = nullLast
    def serialize(self):
        return [self.type.serialize(), self.name.encode(), self.descending, self.nullLast]


class Schema:
    def __init__(self, name: str, version: int,
        fields: [FieldType], partitionKeyFields: [int], rangeKeyFields: [int]):
        self.name = name
        self.version = version
        self.fields = fields
        self.partitionKeyFields = partitionKeyFields
        self.rangeKeyFields = rangeKeyFields

    def serialize(self):
        return [self.name.encode(),
                self.version,
                [field.serialize() for field in self.fields],
                self.partitionKeyFields,
                self.rangeKeyFields
        ]

class CollectionCapacity:
    def __init__(self, minNodes, dataCapacityMegaBytes=0, readIOPs=0, writeIOPs=0):
        self.dataCapacityMegaBytes = dataCapacityMegaBytes
        self.readIOPs = readIOPs
        self.writeIOPs = writeIOPs
        self.minNodes = minNodes

    def serialize(self):
        return [self.dataCapacityMegaBytes, self.readIOPs, self.writeIOPs, self.minNodes]


class HashScheme(Enum):
    Range      = 0
    HashCRC32C = 1

    def serialize(self):
        return self.value


class StorageDriver(int, Enum):
    K23SI = 0

    def serialize(self):
        return self.value


class CollectionMetadata:
    def __init__(self, name: str, hashScheme: HashScheme,
        storageDriver: StorageDriver, capacity: CollectionCapacity,
        retentionPeriod=timedelta(hours=1), heartbeatDeadline=timedelta(hours=0), deleted: bool = False):
        self.name = name
        self.hashScheme = hashScheme
        self.storageDriver = storageDriver
        self.capacity = capacity
        self.retentionPeriod = retentionPeriod
        self.heartbeatDeadline = heartbeatDeadline
        self.deleted = deleted

    def serialize(self):
        return [
            self.name.encode(),
            self.hashScheme.serialize(),
            self.storageDriver.serialize(),
            self.capacity.serialize(),
            serializeTD(self.retentionPeriod),
            serializeTD(self.heartbeatDeadline),
            self.deleted
            ]


class FieldValue(dict):
    def __init__(self, type: FieldType, value: object):
        dict.__init__(self, type = type, value = value)

class SKVClient:
    "SKV DB client"

    def __init__(self, url: str):
        self.http = url

    def _make_call(self, path, data):
        url = self.http + path
        try:
            datab = msgpack.packb(data, use_bin_type=True)
        except Exception as exc:
            return (Status([501, "Unable to serialize request due to: " + str(exc)]), None)
        resp = requests.post(url, data=datab, headers={'Content-Type': 'application/x-msgpack', 'Accept': 'application/x-msgpack'})

        if resp.status_code != 200:
            return (Status([503, "Unable to issue call with data: " + str(data) + ", due to: " + str(resp)]), None)
        try:
            status, obj = msgpack.unpackb(resp.content)
            return (Status(status), obj)
        except Exception as exc:
            return (Status([501, "Unable to deserialize response due to: " + str(exc)]), None)

    def get_schema(self, collectionName: str, schemaName: str,
        schemaVersion: int = -1):
        url = self.http + "/api/GetSchema"
        data = {"collectionName": collectionName, "schemaName": schemaName,
            "schemaVersion": schemaVersion}
        r = requests.post(url, data=json.dumps(data))
        result = r.json()
        status = Status(result)
        if "response" in result:
            schema = Schema(**result["response"])
            return status, schema
        return status, None

    def create_collection(self, md: CollectionMetadata, rangeEnds: [str] = None) -> Status:
        status, _ = self._make_call('/api/CreateCollection', [md.serialize(), rangeEnds if rangeEnds else []])
        return status

    def create_schema(self, collectionName: str, schema: Schema) -> Status:
        status, _ = self._make_call('/api/CreateSchema', [collectionName.encode(), schema.serialize()])
        return status

    def begin_txn(self):
        data = {}
        url = self.http + "/api/BeginTxn"
        r = requests.post(url, data=json.dumps(data))
        result = r.json()
        status = Status(result)
        if "response" in result:
            txn = Txn(self, result["response"].get("txnID"))
        else:
            txn = None
        return status, txn

    def create_query(self, collectionName: str,
        schemaName: str, start: dict = None, end: dict = None,
        limit: int = 0, reverse: bool = False):
        url = self.http + "/api/CreateQuery"
        data = {"collectionName": collectionName,
            "schemaName": schemaName}
        if start:
            data["startScanRecord"] = start
        if end:
            data["endScanRecord"] = end
        if limit:
            data["limit"] = limit
        if reverse:
            data["reverse"] = reverse

        r = requests.post(url, data=json.dumps(data))
        result = r.json()
        status = Status(result)
        if "response" in result:
            return status, Query(result["response"]["queryID"])
        return status, None

    def get_key_string(self, fields: [FieldValue]):
        url = self.http + "/api/GetKeyString"
        req = {"fields": fields}
        data = json.dumps(req)
        r = requests.post(url, data=data)
        result = r.json()
        output = result["response"]["result"] if "response" in result else None
        return Status(result), output


class Counter:
    def __init__(self, module: str, context: str, name: str, short_name=None):
        self.module = module
        self.context = context
        self.name = name
        self.short_name = short_name if short_name else name

class Histogram (Counter):
    pass

class MetricsSnapShot:

    def _set_counter(self, text: str, c: Counter):
        name = f"{c.module}_{c.context}_{c.name}"
        expr = f"^{name}.*\s(\d+)"
        match = re.search(expr, text, re.MULTILINE)
        val = 0 if match is None else int(match.group(1))
        setattr(self, c.short_name, val)

    # format HttpProxy_K23SI_client_write_latency_bucket{le="9288586.978427",shard="0"} 7
    def _set_histogram(self, text: str, h: Histogram):
        search = f'{h.module}_{h.context}_{h.name}_bucket{{le="+Inf"'
        escape = re.escape(search)
        expr = f"^{escape}.*\s(\d+)"
        match = re.search(expr, text, re.MULTILINE)
        val = 0 if match is None else int(match.group(1))
        setattr(self, h.short_name, val)

class MetricsClient:
    def __init__(self, url: str,
            metrices: [Counter]):
        self.http = url
        self.metrices = metrices

    def refresh(self) -> MetricsSnapShot:
        url = self.http + "/metrics"
        r = requests.get(url)
        text = r.text
        metrics = MetricsSnapShot()
        for m in self.metrices:
            if isinstance(m, Histogram):
                metrics._set_histogram(text, m)
            else:
                metrics._set_counter(text, m)
        return metrics
