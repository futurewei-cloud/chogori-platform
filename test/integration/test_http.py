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
from skvclient import (CollectionMetadata, CollectionCapacity, SKVClient, HashScheme, StorageDriver, Schema, SchemaField, FieldType)
from datetime import timedelta
import logging

class TestHTTP(unittest.TestCase):
    args = None
    cl = None
    schema = None

    @classmethod
    def setUpClass(cls):
        "Create common schema and collection used by multiple test cases"
        logging.basicConfig(format='%(asctime)s [%(levelname)s] (%(module)s) %(message)s', level=logging.DEBUG)
        metadata = CollectionMetadata(
            name = 'HTTPClient',
            hashScheme = HashScheme.HashCRC32C,
            storageDriver = StorageDriver.K23SI,
            capacity = CollectionCapacity(minNodes = 2),
            retentionPeriod = timedelta(hours=5)
        )
        TestHTTP.cl = SKVClient(TestHTTP.args.http)
        status = TestHTTP.cl.create_collection(metadata)
        if not status.is2xxOK():
            raise Exception(status.message)

        TestHTTP.schema = Schema(
            name='test_schema',
            version=1,
            fields=[
                SchemaField(FieldType.STRING, 'partitionKey'),
                SchemaField(FieldType.STRING, 'rangeKey'),
                SchemaField(FieldType.STRING, 'data')],
            partitionKeyFields=[0],
            rangeKeyFields=[1]
        )
        status = TestHTTP.cl.create_schema("HTTPClient", TestHTTP.schema)
        if not status.is2xxOK():
            raise Exception(status.message)

    def test_basicTxn(self):
        # Begin Txn
        self.assertTrue(True)
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

'''
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
        self.assertEqual(status.code, 400)
        status, _ = txn.read(bad_loc)
        self.assertEqual(status.code, 400)

        # Write/Read with bad range key data type, should fail
        bad_loc = loc.get_new(range_key=1)
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 400)

        # Write with bad data field data type, should fail
        status = txn.write(loc, {"data": 1})
        self.assertEqual(status.code, 400)

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

    def test_collection_schema_basic(self):
        db = SKVClient(args.http)
        SEC_TO_MICRO = 1000000
        metadata = CollectionMetadata(name = 'HTTPProxy1',
            hashScheme = HashScheme("HashCRC32C"),
            storageDriver = StorageDriver("K23SI"),
            capacity = CollectionCapacity(minNodes = 1),
            retentionPeriod = int(timedelta(hours=5).total_seconds()*SEC_TO_MICRO)
        )
        status = db.create_collection(metadata)
        self.assertEqual(status.code, 200, msg=status.message)

        schema = Schema(name='tests', version=1,
            fields=[
                SchemaField(FieldType.STRING, 'pkey1'),
                SchemaField(FieldType.INT32T, 'rkey1'),
                SchemaField(FieldType.STRING, 'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[1])
        status = db.create_schema("HTTPProxy1", schema)
        self.assertEqual(status.code, 200, msg=status.message)

        status, schema1 = db.get_schema("HTTPProxy1", "tests", 1)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(schema, schema1)

        # Get a non existing schema, should fail
        status, _ = db.get_schema("HTTPProxy1", "tests_1", 1)
        self.assertEqual(status.code, 404, msg=status.message)

        # Create a location object
        loc = DBLoc(partition_key_name="pkey1", range_key_name="rkey1",
            partition_key="ptest4", range_key=4,
            schema="tests", coll="HTTPProxy1", schema_version=1)
        additional_data =  { "datafield1" :"data4"}

        # Populate data, Begin Txn
        status, txn = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Write initial data
        status = txn.write(loc, additional_data)
        self.assertEqual(status.code, 201)

        status, record = txn.read(loc)
        self.assertEqual(status.code, 200)
        self.assertEqual(record["datafield1"], "data4")
        self.assertEqual(record["pkey1"], "ptest4")
        self.assertEqual(record["rkey1"], 4)

        # Commit Txn, should succeed
        status = txn.end()
        self.assertEqual(status.code, 200)

    def test_create_schema_validation(self):
        db = SKVClient(args.http)
        # Create schema with no field, should fail
        schema = Schema(name='tests2', version=1,
            fields=[], partitionKeyFields=[], rangeKeyFields=[])

        status = db.create_schema("HTTPClient", schema)
        self.assertEqual(status.code, 400, msg=status.message)

        # No partition Key, should fail
        schema = Schema(name='tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, 'pkey1'),
                SchemaField(FieldType.INT32T, 'rkey1'),
                SchemaField(FieldType.STRING, 'datafield1')],
            partitionKeyFields=[], rangeKeyFields=[1])

        status = db.create_schema("HTTPClient", schema)
        self.assertEqual(status.code, 400, msg=status.message)

        # Duplicate Key, should fail
        schema = Schema(name='tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, 'pkey1'),
                SchemaField(FieldType.INT32T, 'pkey1'),
                SchemaField(FieldType.STRING, 'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[])

        status = db.create_schema("HTTPClient", schema)
        self.assertEqual(status.code, 400, msg=status.message)

        schema = Schema(name='tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, 'pkey1'),
                SchemaField(FieldType.INT32T, 'rkey1'),
                SchemaField(FieldType.STRING, 'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[1])

        status = db.create_schema("HTTPClient", schema)
        self.assertEqual(status.code, 200, msg=status.message)

        # Create the same schema again, should fail
        status = db.create_schema("HTTPClient", schema)
        self.assertEqual(status.code, 403, msg=status.message)

    def test_query(self):
        db = SKVClient(args.http)
        SEC_TO_MICRO = 1000000
        metadata = CollectionMetadata(name = 'query_collection',
            hashScheme = HashScheme("Range"),
            storageDriver = StorageDriver("K23SI"),
            capacity = CollectionCapacity(minNodes = 2),
            retentionPeriod = int(timedelta(hours=5).total_seconds()*SEC_TO_MICRO)
        )

        status, endspec = db.get_key_string([
            FieldValue(FieldType.STRING, "default"),
            FieldValue(FieldType.STRING, "d")])
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(endspec, "^01default^00^01^01d^00^01")

        # TODO: Have range ends calculated by python or http api.
        status = db.create_collection(metadata,
            rangeEnds = [endspec, ""])
        self.assertEqual(status.code, 200, msg=status.message)

        schema = Schema(name='query_test', version=1,
            fields=[
                SchemaField(FieldType.STRING, 'partition'),
                SchemaField(FieldType.STRING, 'partition1'),
                SchemaField(FieldType.STRING, 'range'),
                SchemaField(FieldType.STRING, 'data1')],
            partitionKeyFields=[0, 1], rangeKeyFields=[2])
        status = db.create_schema("query_collection", schema)
        self.assertEqual(status.code, 200, msg=status.message)

        # Create a location object
        loc = DBLoc(partition_key_name=["partition", "partition1"], range_key_name="range",
            partition_key=["default", "a"], range_key="rtestq",
            schema="query_test", coll="query_collection", schema_version=1)

        # Populate initial data, Begin Txn
        status, txn = db.begin_txn()
        self.assertEqual(status.code, 201)

        # Write initial data
        status = txn.write(loc,  { "data1" :"dataq"})
        self.assertEqual(status.code, 201, msg=status.message)
        status, out = txn.read(loc)
        self.assertEqual(status.code, 200, msg=status.message)
        record1 = {"partition": "default", "partition1": "a", "range" : "rtestq", "data1" : "dataq"}
        self.assertEqual(out, record1)

        loc1 = loc.get_new(partition_key=["default", "h"], range_key="arq1")
        status = txn.write(loc1, { "data1" :"adq1"})
        self.assertEqual(status.code, 201, msg=status.message)
        status, out = txn.read(loc1)
        self.assertEqual(status.code, 200, msg=status.message)
        record2 = {"partition": "default", "partition1": "h", "range" : "arq1", "data1" : "adq1"}
        self.assertEqual(out, record2)

        # Commit initial data
        status = txn.end()
        self.assertEqual(status.code, 200)

        status, txn = db.begin_txn()
        self.assertEqual(status.code, 201)

        all_records = [record1, record2]

        status, query_id = db.create_query("query_collection", "query_test")
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(records, all_records)

        status, query_id = db.create_query("query_collection", "query_test",
            start = {"partition": "default", "partition1": "h"})
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(records, all_records[1:])

        status, query_id = db.create_query("query_collection", "query_test",
            end = {"partition": "default", "partition1": "h"})
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(records, all_records[:1])

        status, query_id = db.create_query("query_collection", "query_test", limit = 1)
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(records, all_records[:1])

        status, query_id = db.create_query("query_collection", "query_test", reverse = True)
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        copied = all_records.copy()
        copied.reverse()
        self.assertEqual(records, copied)

        status, query_id = db.create_query("query_collection", "query_test",
            limit = 1, reverse = True)
        self.assertEqual(status.code, 200, msg=status.message)
        status, records = txn.queryAll(query_id)
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(records,  all_records[1:])

        # Send reverse with invalid type, should fail with type error
        status, query_id = db.create_query("query_collection", "query_test",
            limit = 1, reverse = 5)
        self.assertEqual(status.code, 500, msg=status.message)
        self.assertIn("type_error", status.message, msg=status.message)

        # Send limit with invalid type, should fail with type error
        status, query_id = db.create_query("query_collection", "query_test",
            limit = "test", reverse = False)
        self.assertEqual(status.code, 500, msg=status.message)
        self.assertIn("type_error", status.message, msg=status.message)

    def test_key_string(self):
        db = SKVClient(args.http)
        field1 = FieldValue(FieldType.STRING, "default")
        field2 = FieldValue(FieldType.STRING, "d\x00ef")
        status, endspec = db.get_key_string([field1, field2])
        self.assertEqual(status.code, 200, msg=status.message)
        print(field2)
        self.assertEqual(endspec, "^01default^00^01^01d^00^ffef^00^01")

        field3 = FieldValue(FieldType.INT32T, 10)
        status, endspec = db.get_key_string([field1, field3])
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(endspec, "^01default^00^01^02^03^00^0a^00^01")

    def test_metrics(self):
        "Verify some metrics are populated"
        mclient = MetricsClient(args.prometheus, [
            Counter("HttpProxy", "session", "open_txns"),
            Counter("HttpProxy", "session", "deserialization_errors"),
            Histogram("HttpProxy", "K23SI_client", "txn_begin_latency"),
            Histogram("HttpProxy", "K23SI_client", "txn_end_latency"),
            Histogram("HttpProxy", "K23SI_client", "txn_duration")
            ]
        )
        db = SKVClient(args.http)

        prev = mclient.refresh()
        status, txn = db.begin_txn()
        curr = mclient.refresh()
        self.assertEqual(curr.open_txns, prev.open_txns+1)

        loc = DBLoc(partition_key_name="partitionKey", range_key_name="rangeKey",
            partition_key="ptest2", range_key="rtest2",
            schema="test_schema", coll="HTTPClient", schema_version=1)
        additional_data =  { "data" :"data1"}

        # Write with bad range key data type, should fail
        bad_loc = loc.get_new(range_key=1)
        status = txn.write(bad_loc, additional_data)
        self.assertEqual(status.code, 400, msg=status.message)
        curr = mclient.refresh()
        self.assertEqual(curr.deserialization_errors, prev.deserialization_errors+1)

        status = txn.end()
        self.assertEqual(status.code, 200, msg=status.message)
        curr = mclient.refresh()
        self.assertEqual(curr.open_txns, prev.open_txns)
        self.assertEqual(curr.txn_begin_latency, prev.txn_begin_latency+1)
        self.assertEqual(curr.txn_end_latency, prev.txn_end_latency+1)
        self.assertEqual(curr.txn_duration, prev.txn_duration+1)
'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", help="HTTP API URL")
    parser.add_argument("--prometheus", default="http://localhost:8089", help="HTTP Proxy Prometheus port")
    TestHTTP.args = parser.parse_args()

    del sys.argv[1:]
    unittest.main()
