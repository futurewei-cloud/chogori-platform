This document describes test scenarios for the Partition requests.

# Background

The test cases currently are constructed in a form of an integration test. The tests are in the test runner `chogori-platform/test/dto/PartitionTest.cpp`, which is a stand-alone application that can send/receive partition related requests against a K2 cluster. The tests are currently executed via the integration tester `chogori-platform/test/integration/test_partition.sh`

New tests can be added to the existing test runner in the same manner as the existing tests

Common notes:

- Generate keys with scenario prefix for range/partition, e.g. keys generated in scenario 05 are all prefixed with SC05_
- Use the Inspect verbs to validate state of transactions and data
- We may have to re-initialize a cluster in some cases. It is probably best for now to just start a new cluster with a new collection, creating collections with say a timestamp prefix so that we don't get collection name collisions
- There are global configurations which multiply the test cases. These are listed here
  - Hash/Range partitioning: we should repeat all tests and make sure they pass for both hash and range-based collections

# Test scenarios

## Scenario 01 - get partition for key through range scheme

### Test setup

- start a cluster with three k2 nodes.
- assign collection with range partition. Set the range of each partition as follows: ["", c), [c, e), [e, "")

### Test cases

| test case                                                    | Expected result                     | Possible fix needed |
| ------------------------------------------------------------ | ----------------------------------- | ------------------- |
| using a null-empty key to get Partition with default reverse and exclusiveKey flag | partition that key belongs          |                     |
| using an empty key to get Partition with default reverse and exclusiveKey flag | First partition                     |                     |
| using a null-empty key to get Partition with reverse flag set to be TRUE and default exclusiveKey flag | partition that key belongs          |                     |
| using an empty key to get Partition with reverse flag set to be TRUE and default exclusiveKey flag | last partition                      |                     |
| using a null-empty key to get Partition with reverse and exclusiveKey flag set to be TRUE. the key is NOT the Start key of any partitions. | partition that key belongs          |                     |
| using a null-empty key to get Partition with reverse and exclusiveKey flag set to be TRUE. the key is the Start key of a partition. | previous partition that key belongs |                     |
| using an empty key to get Partition with reverse and exclusiveKey flag set to be TRUE. | last partition                      |                     |
