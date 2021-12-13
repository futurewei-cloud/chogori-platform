#!/bin/bash
set -e
topname=$(dirname "$0")
source ${topname}/common_defs.sh
cd ${topname}/../..

# start nodepool
./build/src/k2/cmd/nodepool/nodepool ${COMMON_ARGS} -c${#EPS[@]} --tcp_endpoints ${EPS[@]} --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --memory=3G --partition_request_timeout=6s &
nodepool_child_pid=$!

# start persistence
./build/src/k2/cmd/persistence/persistence ${COMMON_ARGS} -c1 --tcp_endpoints ${PERSISTENCE} --prometheus_port 63002 &
persistence_child_pid=$!

# start tso
./build/src/k2/cmd/tso/tso ${COMMON_ARGS} -c1 --tcp_endpoints ${TSO} --prometheus_port 63003 --tso.error_bound=100us --tso.clock_poller_cpu=${TSO_POLLER_CORE} &
tso_child_pid=$!

./build/src/k2/cmd/controlPlaneOracle/cpo_main ${COMMON_ARGS} -c1 --tcp_endpoints ${CPO} --data_dir ${CPODIR} --txn_heartbeat_deadline=10s --prometheus_port 63000 --assignment_timeout=1s --nodepool_endpoints ${EPS[@]} --tso_endpoints ${TSO} --persistence_endpoints ${PERSISTENCE} &
cpo_child_pid=$!


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

sleep 2

NUMWH=1
NUMDIST=1
echo ">>> Starting load ..."
./build/src/k2/cmd/tpcc/tpcc_client ${COMMON_ARGS} -c1 --cpo ${CPO} --data_load true --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63100 --memory=512M --partition_request_timeout=6s --dataload_txn_timeout=600s --do_verification true --num_concurrent_txns=2 --item_table_num_nodes=1 --txn_weights={43,4,4,45,4}

sleep 1

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>> Starting benchmark ..."
./build/src/k2/cmd/tpcc/tpcc_client ${COMMON_ARGS} -c1 --cpo ${CPO} --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63101 --memory=512M --partition_request_timeout=6s  --num_concurrent_txns=1 --do_verification true --delivery_txn_batch_size=10 --txn_weights={43,4,4,45,4}
