#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

# start tso
./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port 63003 --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

#assigning unrealistic error bound to tso
./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} --data_dir ${CPODIR} --txn_heartbeat_deadline=10s --prometheus_port 63000 --assignment_timeout=20ms --tso_endpoints ${TSO} --tso_error_bound=1us &
cpo_child_pid=$!


function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}
  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT

sleep 1

/build/test/integration/test_tso_errorbound_fail.py --prometheus_port=63003
