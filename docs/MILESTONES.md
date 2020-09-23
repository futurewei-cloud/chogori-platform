# Milestones:
* SQL In-memory + TPCC
    - design (Ivan/Jian)
        - API
        - caching
        - compute layer elasticity
    - data model mapping (Ivan/Jian)
        - partition of DB to partitioning of K2
        - row mapping
        - secondary index
    - 3SI correctness (Justin)
        - orphaned WIs
        - state inspect capability
        - cleanup/gc
    - Initial push-down (Justin)
        - specific capability to follow
        - scan support
            - API
            - filter & projection
    - Implementation (ALL)
    - SQL TPCC tester (?)
    - Extra
        - HOT (intern)
        - persistence/plog
        - network data structures
* Persistence support
    - recovery
    - GC
    - load balancing (merge/split)
    - analytics, MP ad-hoc query support
    - WAL is the data?
* Push-down compute support
    - shared data schema model
    - document-oriented storage
    - IR, JIT(Lua), C++ (static pre-compiled)
    - API
    - Used by SQL/ graph
    - stored procedures
    - push-down code/IR
    - access to read/write records
    - ability to route and remote-invoke other instances of compute
    - caching for read results
* Graph
    - need breakdown and proposal
    - API
    - caching
    - compute layer elasticity
* Performance
    - indexer based on HOT
    - task scheduling order
    - lower priority bg tasks
    - use seastar scheduling groups
* Analytics
    - first-class support or separate analytics cluster plugin
    - presto/impala/spark support
* Streaming
    - consistency: sync/async
    - external compatibility (kinesis, kafka)
    - filtering & projection
* IAM
    - Crypto at rest/ in flight
    - Accounts, users, roles
* Tiered storage support (is it part of persistence?)
    - Multi-layer support progressively trading latency for cost & size
