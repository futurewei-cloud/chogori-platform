ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2builder:latest
ARG USE_PROXY
RUN proxy apt update && proxy apt install -y libseastar-k2
COPY tcp_server /usr/local/bin/
COPY tcp_client /usr/local/bin/
