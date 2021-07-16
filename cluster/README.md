This directory contains basic python scripts and configuration files to help run Chogori 
performance benchmarks on a cluster of machines.


Requirements:
- passwordless ssh between machine running script and target machines
- passwordless sudo on target machine
- python 3
- python fabric library, e.g. "sudo apt install python3-pip && pip3 install fabric"
- Docker repo with images that contain the Chogori executables


run.py is the top-level script for running the benchmarks. It has commands to start, stop, remove, and 
get logs for the cluster components. For example the full sequence of commands to run the TPC-C benchmark 
could look like this:
1. ./run.py --config\_file configs/cluster.cfg  --start cpo tso persist nodepool
2. ./run.py --config\_file configs/cluster.cfg  --start load
3. ./run.py --config\_file configs/cluster.cfg  --log load
4. ./run.py --config\_file configs/cluster.cfg  --stop load
5. ./run.py --config\_file configs/cluster.cfg  --remove load
6. ./run.py --config\_file configs/cluster.cfg  --start client
7. ./run.py --config\_file configs/cluster.cfg  --log client
8. ./run.py --config\_file configs/cluster.cfg  --stop cpo tso persist nodepool client
9. ./run.py --config\_file configs/cluster.cfg  --remove cpo tso persist nodepool client


Each item listed after --start will start a docker container on the cluster. --stop will stop containers 
but not remove them (so the logs can still be examined), and --remove removes the container. The file 
specified with --config\_file is a top-level configuration file that describes each cluster component 
and where and how to run. The locals.py file has local configuration information that 
must be modified for every cluster environment; it contains hostname URLs and docker image URLs.


Individual configuration files (e.g. configs/cpo.cfg, etc.) contain docker and seastar specific options, 
such as the amount of memory to use. They also contain placeholders for options that are automatically 
configured by the script, for example the TCP endpoints. These placeholder options are prefaced with "$$".


Some guidelines for performance benchmarking:
- Use poll-mode seastar option
- Configure 2MB hugepages (via hugeadm Linux command) and enable the seastar option hugepages
- Use RDMA if available with seastar rmda option
- Use seastar cpuset option to pin to cores, ideally without using hyperthreading
- Turn off swap with Linux "swapoff -a"
