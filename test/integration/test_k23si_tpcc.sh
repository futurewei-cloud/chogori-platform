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

# start CPO on 1 cores
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63000 --assignment_timeout=1s --heartbeat_deadline=1s 2>cpo.log &
cpo_child_pid=$!

# start nodepool on 1 cores
./build/src/k2/cmd/nodepool/nodepool -c1 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --reactor-backend epoll --prometheus_port 63001 --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} --memory=512M --partition_request_timeout=30s 2>nodepool.log &
nodepool_child_pid=$!

# start persistence on 1 cores
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63002 2>persistence.log &
persistence_child_pid=$!

# start tso on 2 cores
./build/src/k2/cmd/tso/tso -c2 --tcp_endpoints ${TSO} 13001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 2>tso.log&
tso_child_pid=$!

function finish {
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
}
trap finish EXIT

sleep 3

NUMWH=1
NUMDIST=1
echo ">>> Starting load ..."
./build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --data_load true --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63100 --enable_tx_checksum true --reactor-backend epoll --memory=512M --partition_request_timeout=30s --dataload_txn_timeout=6000s --do_verification false --num_concurrent_txns=10

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>> Starting benchmark ..."
./build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63101 --enable_tx_checksum true --reactor-backend epoll --memory=512M --partition_request_timeout=1s  --num_concurrent_txns=1 --do_verification false
