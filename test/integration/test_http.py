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
from skvclient import (CollectionMetadata, CollectionCapacity, SKVClient,
    HashScheme, StorageDriver, Schema, SchemaField, FieldType, TimeDelta,
    Operation, Value, Expression, TxnOptions)
import logging
from copy import copy
from time import sleep

class TestHTTP(unittest.TestCase):
    args = None
    cl = None
    schema = None
    cname = b'HTTPClient'

    @classmethod
    def setUpClass(cls):
        "Create common schema and collection used by multiple test cases"
        logging.basicConfig(format='%(asctime)s [%(levelname)s] (%(module)s) %(message)s', level=logging.DEBUG)
        metadata = CollectionMetadata(
            name = TestHTTP.cname,
            hashScheme = HashScheme.HashCRC32C,
            storageDriver = StorageDriver.K23SI,
            capacity = CollectionCapacity(minNodes = 2),
            retentionPeriod = TimeDelta(hours=5)
        )
        TestHTTP.cl = SKVClient(TestHTTP.args.http)
        status = TestHTTP.cl.create_collection(metadata)
        if not status.is2xxOK():
            raise Exception(status.message)

        TestHTTP.schema = Schema(
            name=b'test_schema',
            version=1,
            fields=[
                SchemaField(FieldType.STRING, b'partitionKey'),
                SchemaField(FieldType.STRING, b'rangeKey'),
                SchemaField(FieldType.STRING, b'data')],
            partitionKeyFields=[0],
            rangeKeyFields=[1]
        )
        status = TestHTTP.cl.create_schema(TestHTTP.cname, TestHTTP.schema)
        if not status.is2xxOK():
            raise Exception(status.message)

    def test_basicTxn(self):
        # Begin Txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Write
        record = TestHTTP.schema.make_record(partitionKey=b"test2pk", rangeKey=b"test1rk", data=b"mydata")
        status = txn.write(TestHTTP.cname, record)
        self.assertTrue(status.is2xxOK(), msg=status.message)

        # Abort
        status = txn.end(False)
        self.assertTrue(status.is2xxOK())

        # Begin Txn
        self.assertTrue(True)
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Write
        record = TestHTTP.schema.make_record(partitionKey=b"test1pk", rangeKey=b"test1rk", data=b"mydata")
        status = txn.write(TestHTTP.cname, record)
        self.assertTrue(status.is2xxOK())

        # Read 404
        record = TestHTTP.schema.make_record(partitionKey=b"test2pk", rangeKey=b"test1rk")
        status, resultRec = txn.read(TestHTTP.cname, record)
        self.assertEqual(status.code, 404)

        # read data
        record = TestHTTP.schema.make_record(partitionKey=b"test1pk", rangeKey=b"test1rk")
        status, resultRec = txn.read(TestHTTP.cname, record)
        self.assertTrue(status.is2xxOK());
        self.assertEqual(resultRec.fields.partitionKey, b"test1pk")
        self.assertEqual(resultRec.fields.rangeKey, b"test1rk")
        self.assertEqual(resultRec.fields.data, b"mydata")

        # Commit
        status = txn.end()
        self.assertTrue(status.is2xxOK())

        # Commit again, should fail
        status = txn.end()
        self.assertEqual(status.code, 410)

       # Begin Txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # read data
        record = TestHTTP.schema.make_record(partitionKey=b"test1pk", rangeKey=b"test1rk")
        status, resultRec = txn.read(TestHTTP.cname, record)
        self.assertTrue(status.is2xxOK());
        self.assertEqual(resultRec.fields.partitionKey, b"test1pk")
        self.assertEqual(resultRec.fields.rangeKey, b"test1rk")
        self.assertEqual(resultRec.fields.data, b"mydata")

        # Commit
        status = txn.end()
        self.assertTrue(status.is2xxOK())


    def test_validation(self):
        record = TestHTTP.schema.make_record(partitionKey=b"test2pk", rangeKey=b"test1rk", data=b"mydata")
        # Get a txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Write/read with bad collection name, should fail
        status = txn.write(b"HTTPClient1", record)
        self.assertEqual(status.code, 404)
        status, _ = txn.read(b"HTTPClient1", record)
        self.assertEqual(status.code, 404)

        # # Write/Read with bad schemaName, should fail
        bad_loc = copy(record)
        bad_loc.schemaName = b"test_schema1"
        status = txn.write(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 404)
        status, _ = txn.read(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 404)

        # Write with bad schema version, should fail
        bad_loc = copy(record)
        bad_loc.schemaVersion = 2
        status = txn.write(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 404)

        # Write/Read with bad partition key data type, should fail
        bad_loc = TestHTTP.schema.make_record(partitionKey=1, rangeKey=b"test1rk", data=b"mydata")
        status = txn.write(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 400)
        status, _ = txn.read(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 400)

        # Write/Read with bad range key data type, should fail
        bad_loc = TestHTTP.schema.make_record(partitionKey=b"test2pk", rangeKey=1, data=b"mydata")
        status = txn.write(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 400)

        # Write with bad data field data type, should fail
        bad_loc = TestHTTP.schema.make_record(partitionKey=b"test2pk", rangeKey=1, data=1)
        status = txn.write(TestHTTP.cname, bad_loc)
        self.assertEqual(status.code, 400)

        # End transaction, should succeed
        status = txn.end()
        self.assertTrue(status.is2xxOK())


    # Test read write conflict between two transactions
    def test_read_write_txn(self):
        record = TestHTTP.schema.make_record(partitionKey=b"ptest3", rangeKey=b"rtest3", data=b"data3")

        # additional_data =  { "data" :"data3"}

        # Populate initial data, Begin Txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertEqual(status.code, 201)

        # Write initial data
        status = txn.write(TestHTTP.cname, record)
        self.assertEqual(status.code, 201)

        # Commit initial data
        status = txn.end()
        self.assertEqual(status.code, 200)

        # Begin Txn 1
        status, txn1 = TestHTTP.cl.begin_txn()
        self.assertEqual(status.code, 201)

        # Begin Txn 2
        status, txn2 = TestHTTP.cl.begin_txn()
        self.assertEqual(status.code, 201)

        # Read by Txn 2
        status, resultRec = txn2.read(TestHTTP.cname, record)
        self.assertEqual(status.code, 200)
        self.assertEqual(resultRec.fields.partitionKey, b"ptest3")
        self.assertEqual(resultRec.fields.rangeKey, b"rtest3")
        self.assertEqual(resultRec.fields.data, b"data3")

        # Update data by Txn 1, should fail with 403: write request cannot be allowed as
        # this key (or key range) has been observed by another transaction.
        status = txn1.write(TestHTTP.cname, record)
        self.assertEqual(status.code, 403)

        # Commit Txn 1, same error as write request
        status = txn1.end()
        self.assertEqual(status.code, 403)

        # Commit Txn 2, should succeed
        status = txn2.end()
        self.assertEqual(status.code, 200)

    def test_collection_schema_basic(self):
        test_coll =  b'HTTPProxy1'
        metadata = CollectionMetadata(name =test_coll,
            hashScheme =  HashScheme.HashCRC32C,
            storageDriver = StorageDriver.K23SI,
            capacity = CollectionCapacity(minNodes = 1),
            retentionPeriod = TimeDelta(hours=5)
        )
        status = TestHTTP.cl.create_collection(metadata)
        self.assertTrue(status.is2xxOK(), msg=status.message)

        test_schema = Schema(name=b'tests', version=1,
            fields=[
                SchemaField(FieldType.STRING, b'pkey1'),
                SchemaField(FieldType.INT32T, b'rkey1'),
                SchemaField(FieldType.STRING, b'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[1])
        status = TestHTTP.cl.create_schema(test_coll, test_schema)
        self.assertTrue(status.is2xxOK())

        status, schema1 = TestHTTP.cl.get_schema(test_coll, b"tests", 1)
        self.assertTrue(status.is2xxOK())
        self.assertEqual(test_schema, schema1)

        # Get a non existing schema, should fail
        status, _ = TestHTTP.cl.get_schema(test_coll, b"tests_1", 1)
        self.assertEqual(status.code, 404)

        # Read write using the schema
        record = test_schema.make_record(pkey1=b"ptest4", rkey1=4, datafield1=b"data4")
        # Populate data, Begin Txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Write initial data
        status = txn.write(test_coll, record)
        self.assertTrue(status.is2xxOK())

        status, record1 = txn.read(test_coll, record)
        self.assertTrue(status.is2xxOK())
        self.assertEqual(record.fields.datafield1, b"data4")
        self.assertEqual(record.fields.pkey1, b"ptest4")
        self.assertEqual(record.fields.rkey1, 4)

        # Commit Txn, should succeed
        status = txn.end()
        self.assertTrue(status.is2xxOK())

    def test_create_schema_validation(self):
        # Create schema with no field, should fail
        schema = Schema(name=b'tests2', version=1,
            fields=[], partitionKeyFields=[], rangeKeyFields=[])

        status = TestHTTP.cl.create_schema(TestHTTP.cname, schema)
        self.assertEqual(status.code, 400, msg=status.message)

        # No partition Key, should fail
        schema = Schema(name=b'tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, b'pkey1'),
                SchemaField(FieldType.INT32T, b'rkey1'),
                SchemaField(FieldType.STRING, b'datafield1')],
            partitionKeyFields=[], rangeKeyFields=[1])

        status = TestHTTP.cl.create_schema(TestHTTP.cname, schema)
        self.assertEqual(status.code, 400, msg=status.message)

        # Duplicate Key, should fail
        schema = Schema(name=b'tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, b'pkey1'),
                SchemaField(FieldType.INT32T, b'pkey1'),
                SchemaField(FieldType.STRING, b'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[])

        status = TestHTTP.cl.create_schema(TestHTTP.cname, schema)
        self.assertEqual(status.code, 400, msg=status.message)

        # Valid request, should succeed
        schema = Schema(name=b'tests2', version=1,
            fields=[
                SchemaField(FieldType.STRING, b'pkey1'),
                SchemaField(FieldType.INT32T, b'rkey1'),
                SchemaField(FieldType.STRING, b'datafield1')],
            partitionKeyFields=[0], rangeKeyFields=[1])

        status = TestHTTP.cl.create_schema(TestHTTP.cname, schema)
        self.assertTrue(status.is2xxOK())

        # Create the same schema again, should fail
        status = TestHTTP.cl.create_schema(TestHTTP.cname, schema)
        self.assertEqual(status.code, 403, msg=status.message)

    def test_query(self):
        test_coll = b'query_collection'
        metadata = CollectionMetadata(name = test_coll,
            hashScheme =  HashScheme.Range,
            storageDriver = StorageDriver.K23SI,
            capacity = CollectionCapacity(minNodes = 2),
            retentionPeriod = TimeDelta(hours=5)
        )

        # TODO: Have range ends calculated by python or http api.
        '''
        status, endspec = db.get_key_string([
            FieldValue(FieldType.STRING, "default"),
            FieldValue(FieldType.STRING, "d")])
        self.assertEqual(status.code, 200, msg=status.message)
        self.assertEqual(endspec, "^01default^00^01^01d^00^01")
        '''
        # Use offline calculated range key for now
        endspec = b"^01default^00^01^01d^00^01"
        status = TestHTTP.cl.create_collection(metadata,
            rangeEnds = [endspec, b""])
        self.assertTrue(status.is2xxOK(), msg=status.message)

        test_schema = Schema(name=b'query_test', version=1,
            fields=[
                SchemaField(FieldType.STRING, b'partition'),
                SchemaField(FieldType.STRING, b'partition1'),
                SchemaField(FieldType.STRING, b'range'),
                SchemaField(FieldType.STRING, b'data1'),
                SchemaField(FieldType.INT32T, b'record_id')
                ],
            partitionKeyFields=[0, 1], rangeKeyFields=[2])
        status = TestHTTP.cl.create_schema(test_coll, test_schema)
        self.assertTrue(status.is2xxOK())

        # Populate initial data, Begin Txn
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Write initial data
        record = test_schema.make_record(partition=b"default", partition1=b'a',
            range=b"rtestq", data1=b"dataq", record_id=1)

        status = txn.write(test_coll, record)
        self.assertTrue(status.is2xxOK())
        status, record1 = txn.read(test_coll, record)
        self.assertTrue(status.is2xxOK())
        self.assertEqual(record.data, record1.data)

        record = test_schema.make_record(partition=b"default", partition1=b'h',
            range=b"arq1", data1=b"adq1", record_id=2)
        # loc1 = loc.get_new(partition_key=["default", "h"], range_key="arq1")
        status = txn.write(test_coll, record)
        self.assertTrue(status.is2xxOK())
        status, record2 = txn.read(test_coll, record)
        self.assertTrue(status.is2xxOK())
        self.assertEqual(record.data, record2.data)

        # Commit initial data
        status = txn.end()
        self.assertTrue(status.is2xxOK())

        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Start from partition1=h, should return record2
        key = test_schema.make_record(partition=b"default", partition1=b'h')
        status, query = txn.create_query(test_coll, test_schema.name, start = key)
        self.assertTrue(status.is2xxOK(), msg=status.message)
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record2.data])

        # End to partition1=h, should return record1
        status, query = txn.create_query(test_coll, test_schema.name, end = key)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record1.data])

        # Limit 1
        status, query = txn.create_query(test_coll, test_schema.name, limit = 1)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record1.data])

        # Limit 1 from reverse
        status, query = txn.create_query(test_coll, test_schema.name, limit = 1, reverse = True)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record2.data])

        # Query all, should return record1 and 2
        status, query = txn.create_query(test_coll, test_schema.name)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record1.data, record2.data])

        # Prefix scan on first key field, should return record1 and 2
        key = test_schema.make_prefix_record(partition=b"default")
        status, query = txn.create_query(test_coll, test_schema.name, start = key, end = key)
        self.assertTrue(status.is2xxOK(), msg=status.message)
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record1.data, record2.data])

        # Query all, with reverse = True
        status, query = txn.create_query(test_coll, test_schema.name, reverse = True)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record2.data, record1.data])

        # Do the same query but with filter data1=data1, should only return record2
        filter = Expression(Operation.EQ,
            values = [Value(b"data1"), Value(fieldType=FieldType.STRING, literal=b"adq1")])
        status, query = txn.create_query(test_coll, test_schema.name, filter=filter)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record2.data])

        # Do the same query but with filter record_id=1, should only return record1
        filter = Expression(Operation.EQ,
            values = [Value(b"record_id"), Value(fieldType=FieldType.INT32T, literal=1)])
        status, query = txn.create_query(test_coll, test_schema.name, filter=filter)
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK())
        self.assertEqual([r.data for r in records], [record1.data])
        self.assertNotEqual([r.data for r in records], [record2.data])

        #  Test projection
        status, query = txn.create_query(test_coll, test_schema.name, projection=[b"data1", b"record_id"])
        self.assertTrue(status.is2xxOK())
        status, records = txn.queryAll(query)
        self.assertTrue(status.is2xxOK(), msg=status.message)
        subset = lambda d, keys: {k: d[k] for k in  [b"data1", b"record_id"]}
        self.assertEqual([r.data for r in records], [
            {'partition': None, 'partition1': None, 'range': None, 'data1': b'dataq', 'record_id': 1},
            {'partition': None, 'partition1': None, 'range': None, 'data1': b'adq1', 'record_id': 2}])

        # Send reverse with invalid type, should fail with type error
        status, query = txn.create_query(test_coll, test_schema.name, limit = 1, reverse = 5)
        self.assertEqual(status.code, 400)

        # Send reverse with invalid type, should fail with type error
        status, query = txn.create_query(test_coll, test_schema.name, limit = "test", reverse = 5)
        self.assertEqual(status.code, 400)

        status = txn.end()
        self.assertTrue(status.is2xxOK())

        # Try to make record that is not a prefix, should be caught by python library
        with self.assertRaises(ValueError):
            bad_record = test_schema.make_prefix_record(partition1=b"h")

    def test_txn_timeout(self):
        # Begin Txn with timeout 1s
        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Sleep 1.6s for txn to timeout
        sleep(1.6)
        status = txn.end()
        self.assertEqual(status.code, 410)

        status, txn = TestHTTP.cl.begin_txn()
        self.assertTrue(status.is2xxOK())

        # Sleep 0.8 and write, it should succeed because within timeout
        sleep(0.8)
        record = TestHTTP.schema.make_record(partitionKey=b"ptest6", rangeKey=b"rtest6", data=b"data6")
        status = txn.write(TestHTTP.cname, record)
        self.assertTrue(status.is2xxOK())

        # Sleep additional 0.8s and commit, should succeed as timeout pushed back because of write
        sleep(0.8)
        # Commit
        status = txn.end()
        self.assertTrue(status.is2xxOK())

'''
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
