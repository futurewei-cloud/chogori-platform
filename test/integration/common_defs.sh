set -e
CPODIR=/tmp/___cpo_integ_test
rm -rf ${CPODIR}
EPS=("tcp+k2rpc://0.0.0.0:10000" "tcp+k2rpc://0.0.0.0:10001" "tcp+k2rpc://0.0.0.0:10002" "tcp+k2rpc://0.0.0.0:10003")
NUMCORES=`nproc`
# core on which to run the TSO poller thread. Pick 4 if we have that many, or the highest-available otherwise
#TSO_POLLER_CORE=$(( 5 > $NUMCORES ? $NUMCORES-1 : 4 ))
# poll on random core
TSO_POLLER_CORE=-1
PERSISTENCE=tcp+k2rpc://0.0.0.0:12001
CPO=tcp+k2rpc://0.0.0.0:9000
TSO=tcp+k2rpc://0.0.0.0:13000
COMMON_ARGS="--reactor-backend=epoll --enable_tx_checksum true --thread-affinity false"
HTTP=tcp+k2rpc://0.0.0.0:20000
