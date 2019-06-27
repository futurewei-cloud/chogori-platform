# K2 Benchmark Python Client

## Install

* sudo apt-get update
* sudo apt-get install protobuf-compiler
* pip install protobuf

## Compile

* protoc -I=../../../benchmarker/proto/ --python_out=. ../../../benchmarker/proto/k2bdto.proto

## Run

* python k2b-client.py
