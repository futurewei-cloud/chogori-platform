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
COMMON_ARGS="--poll-mode --thread-affinity false --enable_tx_checksum true"

# start CPO
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --thread-affinity false --prometheus_port 63000 --assignment_timeout=1s --heartbeat_deadline=1s &
cpo_child_pid=$!

# start tso
./build/src/k2/cmd/tso/tso -c1 --tcp_endpoints ${TSO} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 --thread-affinity false --tso.error_bound=100us --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

# start persistence
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63002 &
persistence_child_pid=$!

# start nodepool
./build/src/k2/cmd/nodepool/nodepool -c1 --tcp_endpoints ${EPS} --k23si_persistence_endpoint ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63001 -m 4G --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} &
nodepool_child_pid=$!


function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${nodepool_child_pid}
  echo "Waiting for nodepool child pid: ${nodepool_child_pid}"
  wait ${nodepool_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}

  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT

sleep 5

./build/test/k23si/heartbeat_test --cpo ${CPO} --tcp_remotes ${EPS} ${COMMON_ARGS} -m 16G --prometheus_port 63100 --tso_endpoint ${TSO} --partition_request_timeout=1s --hugepages
