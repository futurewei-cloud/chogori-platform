<!--
    (C)opyright Huawei Technologies USA, 2019
-->
# K2 Application service
K2 Application Service is responsible for execution of application specific logic called Module, implemented through Extensibility framework.
Application Service logic is executed in a Linux processes called Node Pools. Each Node Pool hosts several processing shards called Nodes.

# Structure
* indexer: code related to in-memory indexing data structures, like HOT and ART.
* persistence:  transaction log streams

## Contact

##### Mailing lists

| Name                                                                          | Scope                           |
|:------------------------------------------------------------------------------|:--------------------------------|
| [dfvdenali@huawei.com](mailto:dfvdenali@huawei.com)                           | General discussions             |
