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
import logging
logger = logging.getLogger(__name__)

class TimeDelta(timedelta):
    def serialize(self):
        return int(self.total_seconds()*1000*1000*1000)

class Status:
    "Status returned from HTTP Proxy"

    def __init__(self, status):
        "Get status from status tuple"
        self.code, self.message = status

    def __str__(self):
        return f"{self.code}: {self.message.decode('ascii')}"

    def __repr__(self):
        return str(self)

    def is1xxInfo(self):
        return self.code < 200

    def is2xxOK(self):
        return self.code >= 200 and self.code < 300

    def is3xxRedirect(self):
        return self.code >= 300 and self.code < 400

    def is4xxClientError(self):
        return self.code >= 400 and self.code < 500

    def is5xxServerError(self):
        return self.code >= 500

class Query:
    def __init__(self, query_id: int):
        self.query_id = query_id
        self.done = False

class ExistencePrecondition(int, Enum):
    Nil = 0
    Exists = 1
    NotExists = 2

    def serialize(self):
        return self.value

class TxnPriority(int, Enum):
    Highest =  0
    High =    64
    Medium = 128
    Low =    192
    Lowest = 255

    def serialize(self):
        return self.value

class TxnOptions:
    def __init__(self, timeout=TimeDelta(seconds=10), priority=TxnPriority.Medium, syncFinalize: bool = False):
        self.timeout = timeout
        self.priority = priority
        self.syncFinalize = syncFinalize

    def serialize(self):
        return [self.timeout.serialize(), self.priority.serialize(), self.syncFinalize]

class EndAction(int, Enum):
    NONE:       int     = 0
    ABORT:      int     = 1
    COMMIT:     int     = 2

    def serialize(self):
        return self.value

class Txn:
    "Transaction Object"
    def __init__(self, client, timestamp):
        self.client = client
        self.timestamp = timestamp

    def write(self, collection, record, erase=False, precondition=ExistencePrecondition.Nil) -> Status:
        "Write record"
        record.timestamp = self.timestamp
        status, _ = self.client.make_call('/api/Write',
            [self.timestamp, collection, record.schemaName,
             erase, precondition.serialize(), record.serialize(), record.fieldsForPartialUpdate])
        return status

    def read(self, collection, keyrec) :
        "Read record"
        keyrec.timestamp = self.timestamp
        status, resp = self.client.make_call('/api/Read',
            [self.timestamp, collection, keyrec.schemaName, keyrec.serialize()])
        if not status.is2xxOK() or status.code == 404:
            return status, None

        cname, sname, storage = resp

        status, schema = self.client.get_schema(cname, sname, storage[2])
        if not status.is2xxOK():
            return status, None

        return status, schema.parse_read(storage, self.timestamp)

    def query(self, query: Query):
        request = {"txnID" : self.timestamp, "queryID": query.query_id}
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
        endAction = EndAction.COMMIT if commit else EndAction.ABORT
        status, _ = self.client.make_call('/api/TxnEnd',
            [self.timestamp, endAction.serialize()])
        return status

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


class Data:
    'data storage class for records'
    pass
class Record:
    def __init__(self, schemaName, schemaVersion):
        self.schemaName = schemaName
        self.schemaVersion = schemaVersion
        self._posFields = [] # positional fields
        self.excludedFields=[]
        self.timestamp = None
        self.fieldsForPartialUpdate=[]
        self.fields = Data()

    def serialize(self):
        fieldData = b''.join([msgpack.packb(field) for field in self._posFields])
        return [self.excludedFields, fieldData, self.schemaVersion]

class SchemaField:
    def __init__(self, type: FieldType, name: str,
        descending: bool= False, nullLast: bool = False):
        self.type = type
        self.name = name
        self.descending = descending
        self.nullLast = nullLast

    def serialize(self):
        return [self.type.serialize(), self.name, self.descending, self.nullLast]


class Schema:
    ANY_VERSION=-1
    def __init__(self, name: str, version: int,
        fields: [FieldType], partitionKeyFields: [int], rangeKeyFields: [int]):
        self.name = name
        self.version = version
        self.fields = fields
        self.partitionKeyFields = partitionKeyFields
        self.rangeKeyFields = rangeKeyFields

    def make_record(self, **dataFields):
        rec = Record(self.name, self.version)
        for i,field in enumerate(self.fields):
            dname = field.name.decode('ascii')
            if dname in dataFields:
                fieldValue = dataFields.get(dname)
                rec._posFields.append(fieldValue)
                rec.fields.__dict__[dname] = fieldValue
            else:
                if not rec.excludedFields:
                    rec.excludedFields = [False] * len(self.fields)
                rec.excludedFields[i] = True
                rec.fields.__dict__[dname] = None
        return rec

    def parse_read(self, storage, timestamp):
        rec = Record(self.name, self.version)
        rec.timestamp = timestamp
        rec.excludedFields = storage[0]
        rec.fieldsForPartialUpdate = []
        excl = rec.excludedFields if rec.excludedFields else [False] * len(self.fields)

        unp = msgpack.Unpacker()
        unp.feed(storage[1])
        for i,field in enumerate(self.fields):
            dname = field.name.decode('ascii')
            fv = None
            if not excl[i]:
                fv = unp.unpack()
                rec._posFields.append(fv)

            rec.fields.__dict__[dname] = fv

        return rec

    def serialize(self):
        return [self.name,
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
        retentionPeriod=TimeDelta(hours=1), heartbeatDeadline=TimeDelta(hours=0), deleted: bool = False):
        self.name = name
        self.hashScheme = hashScheme
        self.storageDriver = storageDriver
        self.capacity = capacity
        self.retentionPeriod = retentionPeriod
        self.heartbeatDeadline = heartbeatDeadline
        self.deleted = deleted

    def serialize(self):
        return [
            self.name,
            self.hashScheme.serialize(),
            self.storageDriver.serialize(),
            self.capacity.serialize(),
            self.retentionPeriod.serialize(),
            self.heartbeatDeadline.serialize(),
            self.deleted
            ]


class FieldValue(dict):
    def __init__(self, type: FieldType, value: object):
        dict.__init__(self, type = type, value = value)

class SKVClient:
    "SKV DB client"

    def __init__(self, url: str):
        self.session = requests.Session()
        self.http = url
        self._schemas = {}

    def make_call(self, path, data):
        url = self.http + path
        logger.info("calling {} with data {}".format(url, data))
        try:
            datab = msgpack.packb(data, use_bin_type=True)
        except Exception as exc:
            logger.exception("Unable to serialize request")
            return (Status([501, "Unable to serialize request due to: {}".format(exc)]), None)
        resp = self.session.post(url, data=datab, headers={'Content-Type': 'application/x-msgpack', 'Accept': 'application/x-msgpack'})

        if resp.status_code != 200:
            msg = "Unable to issue call with data: {}, due to: {}".format(data, resp)
            logger.error(msg)
            return (Status([503, msg]), None)
        try:
            status, obj = msgpack.unpackb(resp.content)
            st = Status(status)
            if not st.is2xxOK():
                logger.error("Error response from server: %s", st)

            return (st, obj)
        except Exception as exc:
            logger.exception("Unable to deserialize request")
            return (Status([501, "Unable to deserialize response due to: {}".format(exc)]), None)

    def get_schema(self, collectionName, schemaName, schemaVersion: int = Schema.ANY_VERSION):
        skey = (collectionName, schemaName, schemaVersion)

        if skey not in self._schemas:
            status, schema = self._do_get_schema(collectionName, schemaName, schemaVersion)
            if not status.is2xxOK():
                return status, schema
            self._schemas[skey] = schema

        return Status([200, ""]), self._schemas[skey]

    def _do_get_schema(self, collectionName, schemaName, schemaVersion: int):
        status, schema_raw = self.make_call('/api/GetSchema',
                 [collectionName, schemaName, schemaVersion])
        if not status.is2xxOK():
            return status, None
        fields = [
            SchemaField(FieldType(fraw[0]), fraw[1], fraw[2], fraw[3]) for fraw in schema_raw[2]
        ]

        schema = Schema(schema_raw[0], schema_raw[1],
                        fields, schema_raw[3], schema_raw[4])
        return status, schema

    def create_collection(self, md: CollectionMetadata, rangeEnds: [str] = None) -> Status:
        status, _ = self.make_call('/api/CreateCollection', [md.serialize(), rangeEnds if rangeEnds else []])
        return status

    def create_schema(self, collectionName: str, schema: Schema) -> Status:
        status, _ = self.make_call('/api/CreateSchema', [collectionName, schema.serialize()])
        if status.is2xxOK():
            self._schemas[ (collectionName, schema.name, schema.version) ] = schema
            anykey = (collectionName, schema.name, Schema.ANY_VERSION)
            if anykey not in self._schemas:
                self._schemas[anykey] = schema
        return status

    def begin_txn(self, opts: TxnOptions = None):
        if opts is None:
            opts = TxnOptions()
        status, resp = self.make_call('/api/TxnBegin', [opts.serialize()])
        if status.code == 201:
            return status, Txn(self, resp[0])
        else:
            return status, None

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
