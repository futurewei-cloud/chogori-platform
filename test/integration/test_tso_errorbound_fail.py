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

parser = argparse.ArgumentParser()
parser.add_argument("--prometheus_port", help="TSO prometheus_port")
args = parser.parse_args()
url = "http://127.0.0.1:" + args.prometheus_port + "/metrics"

TEST_TIMEOUT = 2
TEST_RETRY_INTERVAL = 0.1

def get_failed_errbound_times():
    r = requests.get(url)
    count = 0
    for line in r.text.splitlines():
        print(line)
        if "TSOService_TSO_timestamp_errorbound_count" in line:
            try:
                count = int(float(line.split()[1]))
            except:
                continue
    return count

class TestTSOErrorBoundFailure(unittest.TestCase):
    def test_tsoEBFailure(self):
        time_start = time.time()
        count = -1
        while time.time() < time_start + TEST_TIMEOUT:
            try:
                count = get_failed_errbound_times()
                print("count is: ", count)
                if count > 0:
                    break
                time.sleep(TEST_RETRY_INTERVAL)
            except:
                continue
        self.assertTrue(count > 0)

del sys.argv[1:]
unittest.main()
