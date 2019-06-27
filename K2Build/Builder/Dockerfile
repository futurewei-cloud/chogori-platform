ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y libseastar-k2=1.0.6 libhot-k2=0.4 protobuf-compiler
