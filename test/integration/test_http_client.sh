#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --prometheus_port 63000 --assignment_timeout=1s --txn_heartbeat_deadline=1s --nodepool_endpoints ${EPS[@]} --tso_endpoints ${TSO} --tso_error_bound=100us --persistence_endpoints ${PERSISTENCE}&
cpo_child_pid=$!

# start nodepool
./build/src/k2/cmd/nodepool/nodepool ${COMMON_ARGS} -c${#EPS[@]} --tcp_endpoints ${EPS[@]} --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --memory=1G --partition_request_timeout=6s &
nodepool_child_pid=$!

# start persistence
./build/src/k2/cmd/persistence/persistence ${COMMON_ARGS} -c1 --tcp_endpoints ${PERSISTENCE} --prometheus_port 63002 &
persistence_child_pid=$!

# start tso
./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port 63003 --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

sleep 1

# start http proxy with increased cpo request timeout as delete collection takes more time
./build/src/k2/cmd/httpproxy/http_proxy ${COMMON_ARGS} -c1 --tcp_endpoints ${HTTP} --memory=1G --cpo ${CPO} --httpproxy_txn_timeout=100ms --httpproxy_expiry_timer_interval=50ms --cpo_request_timeout=2s&
http_child_pid=$!

function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}

  kill ${http_child_pid}
  echo "Waiting for http child pid: ${http_child_pid}"
  wait ${http_child_pid}

  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT
sleep 1

echo ">>> Starting http test ..."
PYTHONPATH=${PYTHONPATH}:./test/integration ./test/integration/test_http.py --http http://127.0.0.1:30000
