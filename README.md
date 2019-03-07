<!--
    (C)opyright Huawei Technologies USA, 2019
-->
# DFV_K2_Platform
K2 Project is a platform for building low-latency (μs) in-memory distributed persistent OLTP databases.

This repository contains implementations for K2 core services (Application Server, Partition Server, Monitor, Client) and subsystems (transport, persistence, etc.).

## Build

Requirements:
 * make
 * gcc

### Compile and install:

```
mkdir $my_app_dir/build
cd $my_app_dir/build
cmake -DCMAKE_PREFIX_PATH="$seastar_dir/build/release;$seastar_dir/build/release/_cooking/installed" -DCMAKE_MODULE_PATH=$seastar_dir/cmake $my_app_dir
make 
```

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |
|:------------------------------------------------------------------------------|:--------------------------------|
| [dfvdenali@huawei.com](mailto:dfvdenali@huawei.com)                           | General discussions             |
