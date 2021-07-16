from typing import List, Dict


# The smallest unit of deployment target. It is comprised of a single
# network port and its fair share of cores.
class HostNode:
    def __init__(self, dns: str, fastip: str, cores: List[int], rdma: str, config: str):
        """
        :param dns: DNS name of host
        :param fastip: IP address of the network port to listen on
        :param cores: List of core IDs to use
        :param rdma: Name of RDMA port to use, e.g. "mlx5_0"
        :param config: Hardware config of the server
        """
        self.dns = dns
        self.fastip = fastip
        self.cores = cores
        self.rdma = rdma
        self.config = config


host_nodes: List[HostNode] = [
    HostNode("server-1.mydomain.com", "192.168.1.2", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-2.mydomain.com", "192.168.1.4", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-3.mydomain.com", "192.168.1.6", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-4.mydomain.com", "192.168.1.8", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-5.mydomain.com", "192.168.1.10", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-6.mydomain.com", "192.168.1.12", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-7.mydomain.com", "192.168.1.14", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-8.mydomain.com", "192.168.1.16", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-9.mydomain.com", "192.168.1.18", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A"),
    HostNode("server-10.mydomain.com", "192.168.1.20", [10, 11, 12, 13, 14, 15, 16, 17, 18, 19], "mlx5_0", "A")]

port_bases: Dict[str, int] = {"cpo": 7000, "tso": 8000, "persist": 4000, "nodepool": 10000}
