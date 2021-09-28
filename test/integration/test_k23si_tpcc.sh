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
COMMON_ARGS="--enable_tx_checksum true --thread-affinity false"

# start CPO on 1 cores
./build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${CPO} 9001 --data_dir ${CPODIR} ${COMMON_ARGS}  --prometheus_port 63000 --assignment_timeout=1s --reactor-backend epoll --heartbeat_deadline=1s &
cpo_child_pid=$!

# start nodepool on 1 cores
./build/src/k2/cmd/nodepool/nodepool -c1 --tcp_endpoints ${EPS} --k23si_persistence_endpoint ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63001 --k23si_cpo_endpoint ${CPO} --tso_endpoint ${TSO} --memory=3G --partition_request_timeout=6s &
nodepool_child_pid=$!

# start persistence on 1 cores
./build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} ${COMMON_ARGS} --prometheus_port 63002 &
persistence_child_pid=$!

# start tso on 2 cores
./build/src/k2/cmd/tso/tso -c2 --tcp_endpoints ${TSO} 13001 ${COMMON_ARGS} --prometheus_port 63003 &
tso_child_pid=$!

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
  echo ">>>> Test ${0} finished with code ${rv}"
}
trap finish EXIT

sleep 5

NUMWH=1
NUMDIST=1
echo ">>> Starting load ..."
./build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --data_load true --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63100 ${COMMON_ARGS} --memory=512M --partition_request_timeout=6s --dataload_txn_timeout=600s --do_verification true --num_concurrent_txns=2 --txn_weights={43,4,4,45,4}

sleep 1

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>> Starting benchmark ..."
./build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63101 ${COMMON_ARGS} --memory=512M --partition_request_timeout=1s  --num_concurrent_txns=1 --do_verification true --delivery_txn_batch_size=10 --txn_weights={43,4,4,45,4}
