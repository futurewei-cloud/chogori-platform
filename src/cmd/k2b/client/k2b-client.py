#!/usr/bin/python2.7

import sys
import socket
import random
from google.protobuf import text_format
import k2bdto_pb2

host = "localhost"
port = 1300

# connect
sock = socket.socket()
sock.connect((host, port))

# create request
request = k2bdto_pb2.Request()
request.sequenceId = random.randint(1, 100000)
request.operation.type = k2bdto_pb2.Request.Operation.PRIME

# send request
sock.send(request.SerializeToString())

# read socket
response = k2bdto_pb2.Response();
response.ParseFromString(sock.recv(256))
print(text_format.MessageToString(response))
sock.close()
