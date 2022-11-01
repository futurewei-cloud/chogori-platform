#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

allTSOs=("tcp+k2rpc://0.0.0.0:13000" "tcp+k2rpc://0.0.0.0:13001" "tcp+k2rpc://0.0.0.0:13002")
# start tso
./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port 63003 --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} --data_dir ${CPODIR} --txn_heartbeat_deadline=10s --prometheus_port 63000 --assignment_timeout=1s --tso_endpoints ${allTSOs[@]} --tso_error_bound=100us &
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

./test/integration/test_cpo_tso_assign.py --tso_child_pid=${tso_child_pid} --prometheus_port=63000 --cmd="./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c2 --tcp_endpoints \"tcp+k2rpc://0.0.0.0:13001\" \"tcp+k2rpc://0.0.0.0:13002\" --prometheus_port 63004 --tso.clock_poller_cpu=${TSO_POLLER_CORE}"\
