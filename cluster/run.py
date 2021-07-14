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

import argparse

import clusterstate
import parse

from fabric import Connection


def validate_command(runnables, args):
    valid_components = [x.component for x in runnables]
    for name in args.start + args.stop + args.remove + args.logs:
        if name not in valid_components:
            raise RuntimeError(f"Specified component in config does not exist: {name}")


parser = argparse.ArgumentParser(description="Utility script to start/stop/kill cluster components")
parser.add_argument("--config_file", default="configs/cluster.cfg",
                    help="Top-level config file that specifies a Chogori cluster")
parser.add_argument("--state_file", default="state.p", help="Python pickle file of current cluster state")
parser.add_argument("--username", default="user", help="Username to use when SSHing to other nodes")
parser.add_argument("--start", nargs="*", default=[], help="List of component names (from config_file) to be started")
parser.add_argument("--remove", nargs="*", default=[], help="List of component names (from config_file) to be removed")
parser.add_argument("--stop", nargs="*", default=[], help="List of component names (from config_file) to be stopped")
parser.add_argument("--logs", nargs="*", default=[], help="List of component names (from config_file) to display logs")
parser.add_argument("--state", nargs="?", const=True, default=False, help="Show the cluster state")
parser.add_argument("--dry_run", nargs="?", const=True, default=False,
                    help="Change the assumed cluster state without running any ssh commands")
args = parser.parse_args()

runnables = parse.parseConfig(args.config_file)
validate_command(runnables, args)

assignment = clusterstate.Assignment(args.state_file)

for r in runnables:
    pull_cmd = ""
    cmd = ""
    if r.component in args.start or "all" in args.start:
        assignment.add_runnable(r)
        print("Starting:")
        print(r.getDockerRun())
        pull_cmd = r.getDockerPull()
        cmd = r.getDockerRun()
    if r.component in args.stop or "all" in args.stop:
        print("Stopping:")
        print(r)
        cmd = r.getDockerStop()
    if r.component in args.logs or "all" in args.logs:
        print("Getting logs for:")
        print(r)
        cmd = r.getDockerLogs()
    if r.component in args.remove or "all" in args.remove:
        assignment.remove_runnable(r)
        print("Removing:")
        print(r)
        cmd = r.getDockerRemove()

    if not args.dry_run and cmd != "":
        conn = Connection(r.host, user=args.username)
        if pull_cmd != "":
            pull = conn.run(pull_cmd)
            print(pull)
        run = conn.run(cmd)
        print(run)

print(args.state)
if args.state:
    for x in assignment.assignment:
        print(x.runnable)

assignment.save_state(args.state_file)