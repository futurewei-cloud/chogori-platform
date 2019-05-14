#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/..
set -e


./build/src/node/main/node_pool&
child_pid=$!

function finish {
  # cleanup code
  kill ${child_pid}
  echo "Waiting for child pid: ${child_pid}"
  wait ${child_pid}
}
trap finish EXIT

sleep 2

./test/node_test

