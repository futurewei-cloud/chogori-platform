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

echo ">>> Starting load ..."
/build/build/src/k2/cmd/ycsb/ycsb_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --data_load true --prometheus_port 63100 ${COMMON_ARGS} --memory=512M --partition_request_timeout=6s --dataload_txn_timeout=600s --num_concurrent_txns=2 --num_records=500 --num_records_insert=100 --request_dist="latest"

sleep 1

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo ">>> Starting benchmark Workload D..."
/build/build/src/k2/cmd/ycsb/ycsb_client -c1 --tcp_remotes ${EPS} --cpo ${CPO} --tso_endpoint ${TSO} --data_load false --prometheus_port 63100 ${COMMON_ARGS} --memory=512M --partition_request_timeout=6s --num_concurrent_txns=1 --num_records=500 --num_records_insert=100 --test_duration=200ms --ops_per_txn=1 --read_proportion=95 --update_proportion=0 --scan_proportion=0 --insert_proportion=5 --request_dist="latest"
