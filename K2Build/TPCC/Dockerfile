ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2base:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y libboost-thread-dev libboost-system-dev libboost-chrono-dev libboost-program-options-dev libboost-filesystem-dev libboost-test-dev
RUN proxy apt update && proxy apt install -y libcrypto++6 libprotobuf10 libyaml-cpp0.5v5 libnuma1 libhwloc5
RUN proxy apt update && proxy apt-get install -t unstable -y libibverbs-dev=22.1-1
COPY build/src/k2/cmd/tpcc/mock/mock_server /usr/local/bin/
COPY build/src/k2/cmd/tpcc/tpcc_client /usr/local/bin/
COPY build/src/k2/cmd/controlPlaneOracle/cpo_main /usr/local/bin/
COPY build/src/k2/cmd/tso/tso /usr/local/bin/
