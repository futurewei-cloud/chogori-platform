#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}
EPS="tcp+k2rpc://0.0.0.0:10000"
NUMCORES=`nproc`
# core on which to run the TSO poller thread. Pick 4 if we have that many, or the highest-available otherwise
TSO_POLLER_CORE=$(( 5 > $NUMCORES ? $NUMCORES-1 : 4 ))

PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
CPO=tcp+k2rpc://0.0.0.0:9000
TSO=tcp+k2rpc://0.0.0.0:13000
HTTP=tcp+k2rpc://0.0.0.0:20000
COMMON_ARGS="--enable_tx_checksum true --thread-affinity false"

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} ${COMMON_ARGS}  --prometheus_port 63000 --assignment_timeout=1s --reactor-backend epoll --heartbeat_deadline=1s &
cpo_child_pid=$!

# start nodepool
./build/src/k2/cmd/nodepool/nodepool -c1 --tcp_endpoints ${EPS} --k23si_persistence_endpoint ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63001 --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} --memory=1G --partition_request_timeout=6s &
nodepool_child_pid=$!

# start persistence
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63002 &
persistence_child_pid=$!

# start tso
./build/src/k2/cmd/tso/tso -c1 --tcp_endpoints ${TSO} ${COMMON_ARGS} --prometheus_port 63003 --tso.error_bound=100us --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

sleep 3

./build/src/k2/cmd/httpclient/http_client -c1 --tcp_endpoints ${HTTP} --tcp_remotes ${EPS} --memory=1G --cpo ${CPO} --tso_endpoint ${TSO} ${COMMON_ARGS} &
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

sleep 5

echo ">>> Starting http test ..."
./test/integration/test_http.py --http http://127.0.0.1:30000
