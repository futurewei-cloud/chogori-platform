#!/usr/bin/env python3

'''
MIT License

Copyright (c) 2021 Futurewei Cloud

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import argparse, unittest, sys, os, signal, time
import requests, json
from urllib.parse import urlparse

parser = argparse.ArgumentParser()
parser.add_argument("--nodepool_pid", help="Nodepool PID")
parser.add_argument("--prometheus_port", help="CPO prometheus port")
args = parser.parse_args()

#CPOService_HealthMonitor_heartbeats_sent{shard="1",total_cores="2"} 40
#CPOService_HealthMonitor_nodepool_down{shard="1",total_cores="2"} 0.000000
#CPOService_HealthMonitor_nodepool_total{shard="1",total_cores="2"} 1.000000

class TestHeartbeatFailure(unittest.TestCase):
    def test_heartbeatFailure(self):
        url = "http://127.0.0.1:" + args.prometheus_port + "/metrics"
        r = requests.get(url)
        for line in r.text.splitlines():
            if "HealthMonitor_heartbeat_latency_count" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count > 0)
                print("Heartbeats_sent: ", count)
            if "HealthMonitor_nodepool_down" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count == 0)
                print("nodepool_down: ", count)
            if "HealthMonitor_nodepool_total" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count > 0)
                print("nodepool_total: ", count)

        os.kill(int(args.nodepool_pid), signal.SIGKILL)
        time.sleep(2)
            
        url = "http://127.0.0.1:" + args.prometheus_port + "/metrics"
        r = requests.get(url)
        for line in r.text.splitlines():
            if "HealthMonitor_heartbeat_latency_count" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count > 0)
                print("Heartbeats_sent: ", count)
            if "HealthMonitor_nodepool_down" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count == 1)
                print("nodepool_down: ", count)
            if "HealthMonitor_nodepool_total" in line:
                count = 0
                try:
                    count = int(float(line.split()[1]))
                except:
                    continue
                self.assertTrue(count > 0)
                print("nodepool_total: ", count)

# Needed because unittest will throw an exception if it sees any command line arguments it doesn't understand
del sys.argv[1:]
unittest.main()
