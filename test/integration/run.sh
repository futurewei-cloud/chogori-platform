#!/bin/bash
topname=$(dirname "$0")
cd ${topname}

set -e

for test in test_node_pool.sh test_collection.sh; do
    echo ">>> Running integration test: ${test}";
    ./${test};
    echo ">>> Done running test ${test}";
 done
