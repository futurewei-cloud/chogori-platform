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

from parse import Runnable
import locals
import pickle


class AssignedNode:
    def __init__(self, h: locals.HostNode, r: Runnable):
        self.host: locals.HostNode = h
        self.runnable: Runnable = r


class Assignment:
    def __init__(self, pickle_filename, rdma: bool):
        self.use_rdma = rdma
        self.assignment = None
        try:
            with open(pickle_filename, 'rb') as state_p:
                self.assignment = pickle.load(state_p)
        except:
            pass

        if self.assignment is None:
            self.assignment = []
            for node in locals.host_nodes:
                self.assignment.append(AssignedNode(node, None))

    def save_state(self, pickle_filename):
        # If there are no assignments, just save a None object, which
        # will force a reload of the local hosts on the next run
        all_free = True
        for node in self.assignment:
            if node.runnable is not None:
                all_free = False
                break
        if all_free:
            self.assignment = None

        with open(pickle_filename, 'wb') as state_p:
            pickle.dump(self.assignment, state_p)

    def replace_program_arg(self, runnable: Runnable, target_arg, replacement):
        index = -1
        for i in range(0, len(runnable.program_args_list)):
            if runnable.program_args_list[i][1] == target_arg:
                index = i
                break
        if index == -1:
            return

        runnable.program_args_list[index][1] = replacement

    def fill_my_endpoints(self, assigned: AssignedNode):
        try:
            port = locals.port_bases[assigned.runnable.component]
        except:
            # Only server-type components need my_endpoints, for other components we can skip this
            return
        my_endpoints_str = ""
        for i in range(0, assigned.runnable.num_cpus):
            endpoint = f"tcp+k2rpc://{assigned.host.fastip}:{port} "
            port += 1
            my_endpoints_str += endpoint

        self.replace_program_arg(assigned.runnable, "$my_endpoints", my_endpoints_str)

    def fill_rdma(self, assigned: AssignedNode):
        if self.use_rdma:
            self.replace_program_arg(assigned.runnable, "$rdma", assigned.host.rdma)
        else:
            for i in range(0, len(assigned.runnable.program_args_list)):
                if assigned.runnable.program_args_list[i][1] == "$rdma":
                    assigned.runnable.program_args_list.pop(i)
                    break

    def fill_cpuset(self, assigned: AssignedNode):
        cpuset_str = ""
        for i in range(0, assigned.runnable.num_cpus):
            cpuset_str += f"{assigned.host.cores[i]},"
        cpuset_str = cpuset_str[:-1]

        self.replace_program_arg(assigned.runnable, "$cpus_expand", cpuset_str)

    def get_component_endpoints(self, component):
        endpoints = ""
        for node in self.assignment:
            if node.runnable is not None and node.runnable.component == component:
                for arg in node.runnable.program_args_list:
                    if arg[0] == "tcp_endpoints":
                        endpoints += arg[1]
        return endpoints

    def fill_cpo_endpoints(self, assigned: AssignedNode):
        cpo_endpoints = self.get_component_endpoints("cpo")
        if self.use_rdma:
            cpo_endpoints = cpo_endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
        self.replace_program_arg(assigned.runnable, "$cpo_endpoints", cpo_endpoints)

    def fill_nodepool_endpoints(self, assigned: AssignedNode):
        endpoints = self.get_component_endpoints("nodepool")
        self.replace_program_arg(assigned.runnable, "$nodepool_endpoints", endpoints)

    def fill_netserver_endpoints(self, assigned: AssignedNode):
        endpoints = self.get_component_endpoints("netserver")
        if self.use_rdma:
            endpoints = endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
        self.replace_program_arg(assigned.runnable, "$netserver_endpoints", endpoints)

    def fill_rpcserver_endpoints(self, assigned: AssignedNode):
        endpoints = self.get_component_endpoints("rpcserver")
        if self.use_rdma:
            endpoints = endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
        self.replace_program_arg(assigned.runnable, "$rpcserver_endpoints", endpoints)

    def fill_tso_endpoints(self, assigned: AssignedNode):
        tso_endpoints = self.get_component_endpoints("tso")
        # TSO handles its own endpoints for workers, so we just need the master endpoint
        try:
            tso_endpoint = tso_endpoints.split()[0]
        except:
            return
        if self.use_rdma:
            tso_endpoint = tso_endpoint.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
        self.replace_program_arg(assigned.runnable, "$tso_endpoints", tso_endpoint)

    def fill_persist_endpoints(self, assigned: AssignedNode):
        persist_endpoints = self.get_component_endpoints("persist")
        if self.use_rdma:
            persist_endpoints = persist_endpoints.replace("tcp+k2rpc", "auto-rrdma+k2rpc")
        if persist_endpoints == "":
            return
        persist_endpoints_list = persist_endpoints.split()

        shift = assigned.runnable.num_cpus
        if len(persist_endpoints_list) <= shift:
            self.replace_program_arg(assigned.runnable, "$persist_endpoints", persist_endpoints)
            return

        id = int(assigned.runnable.name[len(assigned.runnable.component):])
        for i in range(0, id):
            persist_endpoints_list = persist_endpoints_list[shift:] + persist_endpoints_list[:shift]
        persist_endpoints = ""
        for endpoint in persist_endpoints_list:
            persist_endpoints += endpoint + " "
        self.replace_program_arg(assigned.runnable, "$persist_endpoints", persist_endpoints)

    def make_program_args(self, assigned: AssignedNode):
        for arg in assigned.runnable.program_args_list:
            assigned.runnable.program_args += f"--{arg[0]} {arg[1]} "

    def check_assigned(self, component):
        for node in self.assignment:
            if node.runnable is not None and node.runnable.component == component:
                return True

        return False

    def add_runnable(self, runnable: Runnable):
        # First check for free assignment
        assigned = None
        host: locals.HostNode = None
        for node in self.assignment:
            if node.host.config == runnable.target_config and node.runnable is None and \
                    runnable.num_cpus <= len(node.host.cores):
                assigned = node
                host = node.host
                break
        if assigned is None:
            raise RuntimeError(f"No free hosts for {runnable}")

        # Second check for cluster prerequisites (e.g. CPO must be parsed before nodepool)
        can_assign = False
        if runnable.component == "netserver" or runnable.component == "netclient":
            can_assign = True
        if runnable.component == "rpcserver" or runnable.component == "rpcclient":
            can_assign = True
        # CPO, TSO, and persist are standalone with no prerequisites
        elif runnable.component == "cpo" or runnable.component == "tso" or runnable.component == "persist":
            can_assign = True
        elif runnable.component == "nodepool":
            can_assign = self.check_assigned("cpo") and self.check_assigned("tso") and self.check_assigned("persist")
        else:
            can_assign = self.check_assigned("nodepool")
        if not can_assign:
            raise RuntimeError(f"Cluster prerequisites are not met for {runnable}")

        assigned.runnable = runnable
        runnable.cpus = host.cores[:runnable.num_cpus]
        runnable.host = host.dns
        self.fill_my_endpoints(assigned)
        self.fill_rdma(assigned)
        self.fill_cpuset(assigned)
        self.fill_cpo_endpoints(assigned)
        self.fill_tso_endpoints(assigned)
        self.fill_persist_endpoints(assigned)
        self.fill_nodepool_endpoints(assigned)
        self.fill_netserver_endpoints(assigned)
        self.fill_rpcserver_endpoints(assigned)
        self.make_program_args(assigned)

    def get_assigned_runnable(self, runnable: Runnable):
        index = -1
        for i in range(0, len(self.assignment)):
            if self.assignment[i].runnable is not None and self.assignment[i].runnable.name == runnable.name:
                return self.assignment[i].runnable

        return None

    def remove_runnable(self, runnable: Runnable):
        index = -1
        for i in range(0, len(self.assignment)):
            if self.assignment[i].runnable is not None and self.assignment[i].runnable.name == runnable.name:
                index = i
                break

        if index != -1:
            host = self.assignment[index].host
            r = self.assignment[index].runnable
            self.assignment[index] = AssignedNode(host, None)
            return r
        return None
