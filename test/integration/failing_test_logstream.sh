#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --prometheus_port 63000 &
cpo_child_pid=$!

# start plog
./build/src/k2/cmd/plog/plog_main ${COMMON_ARGS} -c3 -m500M --tcp_endpoints 10000 10001 10002 --prometheus_port=63001 &
plog_child_pid=$!

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

sleep 1

./build/test/plog/logstream_test ${COMMON_ARGS} -m 500M --cpo_url ${CPO} --tcp_endpoints 12345 --plog_server_endpoints tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002 --prometheus_port=63002
