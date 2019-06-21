ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/k2base:latest

ARG USE_PROXY
RUN proxy apt update && proxy apt upgrade -y && proxy apt install -y \
    python-pip \
    vim \
    k2platform;

WORKDIR /usr/local/bin

COPY start.py start.py
CMD ["/usr/local/bin/start.py"]
