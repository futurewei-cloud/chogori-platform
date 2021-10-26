#!/bin/bash
export K2_CPO_ADDRESS=tcp+k2rpc://0.0.0.0:9000
export K2_TSO_ADDRESS=tcp+k2rpc://0.0.0.0:13000

export CPODIR=/tmp/___cpo_dir
export EPS="tcp+k2rpc://0.0.0.0:10000 tcp+k2rpc://0.0.0.0:10001 tcp+k2rpc://0.0.0.0:10002 tcp+k2rpc://0.0.0.0:10003"
export PERSISTENCE=tcp+k2rpc://0.0.0.0:12001

rm -rf ${CPODIR}
export PATH=${PATH}:/usr/local/bin

export K2_LOG_PATH=/tmp/k2_logs
rm -rf ${K2_LOG_PATH}
mkdir ${K2_LOG_PATH}

# start CPO
/build/build/src/k2/cmd/controlPlaneOracle/cpo_main -c1 --tcp_endpoints ${K2_CPO_ADDRESS} --data_dir ${CPODIR} --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63000 --heartbeat_deadline=2s --thread-affinity false --assignment_timeout=500ms > ${K2_LOG_PATH}/cpo.log 2>&1 &
cpo_child_pid=$!

# start nodepool
/build/build/src/k2/cmd/nodepool/nodepool -c4 --tcp_endpoints ${EPS} --enable_tx_checksum true --k23si_persistence_endpoint ${PERSISTENCE} --prometheus_port 63001 --k23si_cpo_endpoint ${K2_CPO_ADDRESS} --tso_endpoint ${K2_TSO_ADDRESS} -m16G --thread-affinity false > ${K2_LOG_PATH}/nodepool.log 2>&1 &
nodepool_child_pid=$!

# start persistence
/build/build/src/k2/cmd/persistence/persistence -c1 --tcp_endpoints ${PERSISTENCE} --enable_tx_checksum true --prometheus_port 63002 --thread-affinity false > ${K2_LOG_PATH}/persistence.log 2>&1 &
persistence_child_pid=$!

# start tso
/build/build/src/k2/cmd/tso/tso -c2 --tcp_endpoints ${K2_TSO_ADDRESS} 13001 --enable_tx_checksum true --reactor-backend epoll --prometheus_port 63003 --thread-affinity false > ${K2_LOG_PATH}/tso.log 2>&1 &
tso_child_pid=$!

/build/promtail/promtail --config.file=/build/promtail/promtail-conf.yml --client.external-labels=instance=$(hostname) > ${K2_LOG_PATH}/promtail.log 2>&1 &
