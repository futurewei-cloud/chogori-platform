<!--
    (C)opyright Huawei Technologies USA, 2019
-->
# DFV_K2_Platform
K2 Project is a platform for building low-latency (μs) in-memory distributed persistent OLTP databases.

This repository contains implementations for K2 core services (Application Server, Partition Server, Monitor, Client) and subsystems (transport, persistence, etc.).

# Structure
*src/common: Shared functionality
*src/node: Application service related code: Node Pool, Node, etc.
*src/manager: Partition manager
*src/client: K2 client

## Build

Requirements:
 * docker
 * make
 * gcc

### Compile and install:

```
make 
```

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |
|:------------------------------------------------------------------------------|:--------------------------------|
| [dfvdenali@huawei.com](mailto:dfvdenali@huawei.com)                           | General discussions             |
