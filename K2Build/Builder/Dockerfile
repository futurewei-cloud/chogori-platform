ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y libseastar-k2=1.0.14 libhot-k2=0.4 libfmt-k2=6.1.2 libcrc32c-k2=1.0.0 protobuf-compiler
