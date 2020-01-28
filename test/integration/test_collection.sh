#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}

# start CPO on 2 cores
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c2 --tcp_endpoints 9000 9001 --cluster_nodes tcp+k2rpc://localhost:10000 tcp+k2rpc://localhost:10001 tcp+k2rpc://localhost:10002 --data_dir ${CPODIR} --enable_tx_checksum true&
cpo_child_pid=$!

# start nodepool on 3 cores
./build/src/k2/cmd/nodepool/nodepool -c3 --tcp_endpoints 10000 10001 10002 --enable_tx_checksum true&
nodepool_child_pid=$!

function finish {
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}
}
trap finish EXIT

sleep 1

./build/test/cpo/cpo_test --cpo_endpoint tcp+k2rpc://0.0.0.0:9001 --enable_tx_checksum true
