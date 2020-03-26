#!/usr/bin/env python3

import parse
import argparse
from fabric import Connection

parser = argparse.ArgumentParser(description="Utility script to start/stop/kill cluster components")
parser.add_argument("--config_file", help="Top-level config file that specifices a cluster")
parser.add_argument("--start", nargs="*", default="", help="List of component names (from config_file) to be started")
parser.add_argument("--stop", nargs="*", default="", help="List of component names (from config_file) to be stopped")
parser.add_argument("--logs", nargs="*", default="", help="List of component names (from config_file) to display logs")
args = parser.parse_args()

print(args.config_file)
runnables = parse.parseConfig(args.config_file)
for r in runnables:
    if r.name in args.start or "all" in args.start:
        print("Starting:")
        print(r.getDockerRun())
        conn = Connection(r.host, user="user")
        pull = conn.run(r.getDockerPull())
        print(pull)
        start = conn.run(r.getDockerRun())
        print(start)
    if r.name in args.stop or "all" in args.stop:
        print("Stopping:")
        print(r)
        conn = Connection(r.host, user="user")
        start = conn.run(r.getDockerStop())
        print(start)
    if r.name in args.logs or "all" in args.logs:
        print("Getting logs for:")
        print(r)
        conn = Connection(r.host, user="user")
        start = conn.run(r.getDockerLogs())
        print(start)
