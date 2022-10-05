FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt upgrade -y
RUN apt install -y build-essential cmake vim gdb strace psmisc pkg-config python3 python3-pip git sed
RUN pip3 install ipython requests msgpack

CMD ["/bin/bash"]
