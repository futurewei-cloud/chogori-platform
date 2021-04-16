FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt upgrade -y
RUN apt install -y build-essential cmake vim gdb strace psmisc pkg-config python3 python3-pip git sed

CMD ["/bin/bash"]

