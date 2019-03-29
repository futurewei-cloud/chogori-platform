<!--
    (C)opyright Futurewei Technologies Inc, 2019
-->

# DFV_K2_Platform
K2 Project is a platform for building low-latency (μs) in-memory distributed persistent OLTP databases.

This repository contains implementations for K2 core services (Application Server, Partition Server, Monitor, Client) and subsystems (transport, persistence, etc.).

## Build

Requirements:
 * docker
 * git
 * shell

### Build code

```
./K2Build/k2build make -C K2Build
```
### Create a .deb package

```
./K2Build/k2build make -C K2Build package

```
### Run a built binary

```
./K2Build/k2build ./build/benchmark
```
### Get a shell in the build environment

```
./K2Build/k2build
```

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |
|:------------------------------------------------------------------------------|:--------------------------------|
| [dfvdenali@huawei.com](mailto:dfvdenali@huawei.com)                           | General discussions             |
