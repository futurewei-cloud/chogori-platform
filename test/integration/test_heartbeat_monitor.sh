#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}
EPS="tcp+k2rpc://0.0.0.0:10000"

PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
CPO=tcp+k2rpc://0.0.0.0:9000
TSO=tcp+k2rpc://0.0.0.0:13000

# start nodepool on 1 cores
./build/src/k2/cmd/nodepool/nodepool --log_level INFO k2::skv_server=INFO -c1 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --reactor-backend epoll --prometheus_port 63001 --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} --partition_request_timeout=30s &
nodepool_child_pid=$!

# start persistence on 1 cores
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63002 &
persistence_child_pid=$!

# start tso on 2 cores
./build/src/k2/cmd/tso/tso -c2 --tcp_endpoints ${TSO} 13001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 &
tso_child_pid=$!

sleep 2

# start CPO on 2 cores
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c2 --tcp_endpoints ${CPO} --data_dir ${CPODIR} --txn_heartbeat_deadline=10s --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63000 --assignment_timeout=1s --nodepool_endpoints ${EPS} --tso_endpoints ${TSO} --persistence_endpoints ${PERSISTENCE} &
cpo_child_pid=$!


function finish {
  rv=$?
  # cleanup code
  rm -rf ${CPODIR}

  kill ${cpo_child_pid}
  echo "Waiting for cpo child pid: ${cpo_child_pid}"
  wait ${cpo_child_pid}

  kill ${persistence_child_pid}
  echo "Waiting for persistence child pid: ${persistence_child_pid}"
  wait ${persistence_child_pid}

  kill ${tso_child_pid}
  echo "Waiting for tso child pid: ${tso_child_pid}"
  wait ${tso_child_pid}
  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT

sleep 2

/build/test/integration/heartbeat_monitor.py --nodepool_pid=${nodepool_child_pid} --prometheus_port=63000
