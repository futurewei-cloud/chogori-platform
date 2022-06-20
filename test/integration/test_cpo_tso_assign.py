#!/usr/bin/env python3

'''
MIT License

Copyright (c) 2022 Futurewei Cloud

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
import shlex, subprocess


parser = argparse.ArgumentParser()
parser.add_argument("--tso_child_pid", help="TSO child PID")
parser.add_argument("--prometheus_port", help="CPO prometheus port")
parser.add_argument("--cmd", help="Command to run the new TSOs")
args = parser.parse_args()

def readMetrics():
    url = "http://127.0.0.1:" + args.prometheus_port + "/metrics"
    r = requests.get(url)
    success_count = 0
    failure_count = 0
    for line in r.text.splitlines():
        if "CPOService_CPO_assigned_tso_instances" in line:
            try:
                success_count = int(float(line.split()[1]))
            except:
                continue
            print("CPOService_CPO_assigned_tso_instances: ", success_count)

        if "CPOService_CPO_unassigned_tso_instances" in line:
            print("CPOService_CPO_unassigned_tso_instances: ", line)
            try:
                failure_count = int(float(line.split()[1]))
                print("CPOService_CPO_unassigned_tso_instances: ", failure_count)
            except:
                continue
    return success_count, failure_count



class TestTSOAssignFailure(unittest.TestCase):
    ### testing TSO assignment failure. Start with unreachable TSOs. Confirm the retry logic works.
    def test_tsoAssignFailure(self):
        scount, fcount = readMetrics()
        self.assertEqual(scount, 1)
        self.assertEqual(fcount, 2)

        time.sleep(1)
        new_tso_args = shlex.split(args.cmd)
        p = subprocess.Popen(new_tso_args)
        time.sleep(2)

        scount, fcount = readMetrics()
        self.assertEqual(scount, 3)
        self.assertEqual(fcount, 0)

        p.terminate()
        exit_code = p.wait()
        self.assertEqual(exit_code, 0)

del sys.argv[1:]
unittest.main()
