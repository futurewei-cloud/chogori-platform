#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}

CPO=tcp+k2rpc://0.0.0.0:9000

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --prometheus_port 63000 &
cpo_child_pid=$!

sleep 2

# start plog
./build/src/k2/cmd/plog/plog_main -c 3 --tcp_endpoints 10000 10001 10002 --prometheus_port=63001 &
plog_child_pid=$!
sleep 2

function finish {
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${plog_child_pid}
  echo "Waiting for plog child pid: ${plog_child_pid}"
  wait ${plog_child_pid}
}
trap finish EXIT


./build/test/plog/plog_test  --cpo_url ${CPO} --tcp_endpoints 12345 --plog_server_endpoints tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002 --prometheus_port=63002
