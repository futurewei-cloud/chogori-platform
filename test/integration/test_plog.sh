#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --prometheus_port 63000 --assignment_timeout=1s --per_call_tso_assignment_timeout=100ms &
cpo_child_pid=$!

# start plog
./build/src/k2/cmd/plog/plog_main ${COMMON_ARGS} -c3 --tcp_endpoints 10000 10001 10002 --prometheus_port=63001 &
plog_child_pid=$!

function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${plog_child_pid}
  echo "Waiting for plog child pid: ${plog_child_pid}"
  wait ${plog_child_pid}
  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT

sleep 1

./build/test/plog/plog_test ${COMMON_ARGS} --cpo_url ${CPO} --tcp_endpoints 12345 --plog_server_endpoints tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002 --prometheus_port=63002
