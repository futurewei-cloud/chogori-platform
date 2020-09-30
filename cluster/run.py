#!/usr/bin/env python3

'''
MIT License

Copyright (c) 2020 Futurewei Cloud

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

import parse
import argparse
from fabric import Connection

parser = argparse.ArgumentParser(description="Utility script to start/stop/kill cluster components")
parser.add_argument("--config_file", help="Top-level config file that specifices a Chogori cluster")
parser.add_argument("--locals_file", help="Top-level config file that specifices local environment")
parser.add_argument("--username", default="user", help="Username to use when SSHing to other nodes")
parser.add_argument("--start", nargs="*", default="", help="List of component names (from config_file) to be started")
parser.add_argument("--remove", nargs="*", default="", help="List of component names (from config_file) to be removed")
parser.add_argument("--stop", nargs="*", default="", help="List of component names (from config_file) to be stopped")
parser.add_argument("--logs", nargs="*", default="", help="List of component names (from config_file) to display logs")
args = parser.parse_args()

args.config_file
runnables = parse.parseConfig(args.locals_file, args.config_file)
for r in runnables:
    if r.name in args.start or "all" in args.start:
        print("Starting:")
        print(r.getDockerRun())
        conn = Connection(r.host, user=args.username)
        pull = conn.run(r.getDockerPull())
        print(pull)
        start = conn.run(r.getDockerRun())
        print(start)
    if r.name in args.stop or "all" in args.stop:
        print("Stopping:")
        print(r)
        conn = Connection(r.host, user=args.username)
        start = conn.run(r.getDockerStop())
        print(start)
    if r.name in args.logs or "all" in args.logs:
        print("Getting logs for:")
        print(r)
        conn = Connection(r.host, user=args.username)
        start = conn.run(r.getDockerLogs())
        print(start)
    if r.name in args.remove or "all" in args.remove:
        print("Removing:")
        print(r)
        conn = Connection(r.host, user=args.username)
        start = conn.run(r.getDockerRemove())
        print(start)
