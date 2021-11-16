#!/bin/bash
topname=$(dirname "$0")
cd ${topname}

set -e

for test in test_*.sh; do
    echo ">>> Running integration test: ${test}";
    ./${test};
    echo ">>> Done running test ${test}";
 done
