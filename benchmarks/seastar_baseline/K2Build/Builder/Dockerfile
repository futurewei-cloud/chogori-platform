ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2seastar_builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y libseastar-k2
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig/
RUN proxy apt update && proxy apt install -y libdpdk-k2-dev
