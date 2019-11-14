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
| [MS Teams](https://teams.microsoft.com/l/channel/19%3a80ad8dec4e364c0196f5422e5cd6af70%40thread.skype/K2-Public?groupId=4bc52ade-0b7d-40b6-a20a-7e8e66545532&tenantId=0fee8ff2-a3b2-4018-9c75-3a1d5591fedc)                           | General discussions             |
