This document describes test scenarios for the Query requests.

# Background

The test cases currently are constructed in a form of an integration test. The tests are in the test runner `chogori-platform/test/k23si/QueryTest.cpp`, which is a stand-alone application that can send/receive query requests against a K2 cluster. The tests are currently executed via the integration tester `chogori-platform/test/integration/test_query.sh`

New tests can be added to the existing test runner in the same manner as the existing tests

Common notes:

- Generate keys with scenario prefix for range/partition, e.g. keys generated in scenario 05 are all prefixed with SC05_
- Use the Inspect verbs to validate state of transactions and data
- We may have to re-initialize a cluster in some cases. It is probably best for now to just start a new cluster with a new collection, creating collections with say a timestamp prefix so that we don't get collection name collisions
- There are global configurations which multiply the test cases. These are listed here
  - Hash/Range partitioning: we should repeat all tests and make sure they pass for both hash and range-based collections

# Test scenarios

## Scenario 01 - Forward scan queries with no filter or projection 

### Test setup

- start a cluster, set the 'k23si_query_pagination_limit' server option to 2.
- assign collection with range partition. make "d" the end of the first range (two partitions in all). 
- create schema: {"partition":string, "range":string}. then insert records: ("a", ""), ("b", ""), ("c", ""), ("d", ""), ("e", ""), ("f", "").

### Test cases

| test case | Expected result | Possible fix needed |
| --------- | --------------- | ------------------- |
|           |                 |                     |

## Scenario 02 - Reverse scan queries with no filter or projection

### Test setup

- same as Scenario 01.

### Test cases

| test case                                                    | Expected result                                              | Possible fix needed |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------- |
| reverse scan within a single partition and single pagination --> [c, a) | returns one pagination of two records --> "c", "b"           |                     |
| reverse scan a limited number (1-record) of SKVRecords in a single partition --> [d, a) | returns one pagination of one records --> "d"                |                     |
| reverse scan in a single partition with no SKVRecords found --> [abz, ab) | returns one pagination of no records --> ""                  |                     |
| reverse scan in a single partition, where the end key happens to be the partition's start key --> [f, d) | returns one pagination of two records --> "f", "e"           |                     |
| reverse scan where the end key is just a little smaller than the partition's Start key --> [f, c) | return three paginations of three records --> "f", "e"; "d" and an empty pagination |                     |
| reverse scan in multi-partitions with limited number (5-record) of SKVRecords --> [f, "") | return three paginations of five records --> "f", "e"; "d"; "c", "b" |                     |
| reverse scan in multi-partitions, terminated by end key --> [f, baby) | return three paginations of four records --> "f", "e"; "d"; "c" |                     |
| reverse scan in multi-partitions with full SKVRecords --> ["", ""); <br />check the order of the results. | returns four paginations of six records -->"f", "e"; "d"; "c", "b"; "a" |                     |

## Scenario 03 - Projection queries for the giving field

### Test setup

- same as Scenario 01.
- doQuery by adding projection.

### Test cases

| test case                                                    | Expected result                                              | Possible fix needed |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------- |
| projection reads for the giving field. Only project "partition" field | Only "partition" field payload is returned. Its excludedField for "range" field is set to 1. |                     |
| projection a field that is not part of the schema            | the unknow projection fields are ignored and returns the SKVRecords of the field projection known to the schema |                     |

## Scenario 04 - write in range is blocked by readCache 
### Test setup
- 
### Test cases

| test case | Expected result | Possible fix needed |
| --------- | --------------- | ------------------- |
|           |                 |                     |
## Scenario 05 - Todo: dealing with query requests while changing the partition map

### Test setup

- 

### Test cases

| test case | Expected result | Possible fix needed |
| --------- | --------------- | ------------------- |
|           |                 |                     |
