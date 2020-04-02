#!/bin/bash
topname=$(dirname "$0")
cd ${topname}

set -e

for test in test_collection.sh test_k23si.sh; do
    echo ">>> Running integration test: ${test}";
    ./${test};
    echo ">>> Done running test ${test}";
 done
