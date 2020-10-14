#!/bin/bash
topname=$(dirname "$0")
cd ${topname}

set -e

for test in test_collection.sh test_k23si.sh test_3si_txn.sh test_schema_create.sh test_skv_client.sh test_query.sh; do
    echo ">>> Running integration test: ${test}";
    ./${test};
    echo ">>> Done running test ${test}";
 done
