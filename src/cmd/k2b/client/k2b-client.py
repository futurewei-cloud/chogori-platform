#!/usr/bin/python2.7

import sys
import socket
import random
import getopt
from google.protobuf import text_format
import k2bdto_pb2

defaultHost = "localhost"
defaultPort = 1300

def invokeBench(benchHost, benchPort, request):
    # connect
    sock = socket.socket()
    sock.connect((benchHost, benchPort))

    # send request
    sock.send(request.SerializeToString())

    # read socket
    response = k2bdto_pb2.Response()
    response.ParseFromString(sock.recv(256))
    print(text_format.MessageToString(response))
    sock.close()

def main(argv):
    benchHost = defaultHost
    benchPort = defaultPort

    try:
        opts, args = getopt.getopt(argv, "hb:", ["bench-host="])
    except getopt.GetoptError:
        print "k2b-client.py -b <benchmarker host>"
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print "k2b-client.py -b <benchmarker host>"
            sys.exit()
        elif opt in ("-b", "--bench-host"):
            benchHost = arg


    # create request
    request = k2bdto_pb2.Request()
    request.sequenceId = random.randint(1, 1000000)
    request.operation.type = k2bdto_pb2.Request.Operation.PRIME
    request.operation.load.count = 1000;

    # start invoke benchmarker
    invokeBench(benchHost, benchPort, request)

if __name__ == "__main__":
    main(sys.argv[1:])
