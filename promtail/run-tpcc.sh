#!/bin/bash
topname=$(dirname "$0")
cd ${topname}/../..
set -e
CPODIR=/tmp/___cpo_integ_test
EPS="tcp+k2rpc://0.0.0.0:10000"

PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
CPO=tcp+k2rpc://0.0.0.0:9000
TSO=tcp+k2rpc://0.0.0.0:13000
COMMON_ARGS="--enable_tx_checksum true --thread-affinity false"

NUMWH=1
NUMDIST=1
echo ">>> Starting load ..."
/build/build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --data_load true --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63100 ${COMMON_ARGS} --memory=512M --partition_request_timeout=6s --dataload_txn_timeout=600s --do_verification true --num_concurrent_txns=2 --txn_weights={43,4,4,45,4}

sleep 1

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>> Starting benchmark ..."
/build/build/src/k2/cmd/tpcc/tpcc_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --num_warehouses ${NUMWH} --districts_per_warehouse ${NUMDIST} --prometheus_port 63101 ${COMMON_ARGS} --memory=512M --partition_request_timeout=6s  --num_concurrent_txns=1 --do_verification true --delivery_txn_batch_size=10 --txn_weights={43,4,4,45,4}
