Team milestones and roadmap

# 2022 Milestone
- timeline: June/July(~6mo)

## Leftovers
- RDMA issues
    - visit lab to inspect HW setup
    - look at drop counters (report metrics)
- Readcache cleanup
- K23SI client library from SQL as a stand-alone integration library
- revisit TPCC-SQL?
- OpenGauss integration(<20%)

## Extras
- start project website with blog (github sites)

## OpenGauss integration
- Fits with industry trends
- Full project familiarity and major rearchitecture work needed
- Main contribution would be making OpenGauss scalable. We've already done this with Postgres
    - secondary contribution: External consistency >> SSI
- Low contribution to project value
- Low research value

## Persistence and Tiered Memory
- Fits with industry trends for lower TCO
- High contribution value to project. Highly useful to project's stability and op-readiness
- Medium research value.
- SCM/NVME design
- PBRB
- PLOG garbage collection
- integrate with LogStream
- Warm replica and failover support
- partition merge/split (cluster scaling)
- software upgrade support
- Snapshot load/store
- Tiered memory
    - seastar-based allocator
    - custom allocator vs memory-mode paired with DRAM
    - DAX (mem-mapped file). Use SCM directly without caching
    - SKV record pool, with no DRAM caching, on top of DAX-SCM
    - server payload copy directly into SCM
    - 
### Tiered Memory Deliverables
- DRAM + SCM tiered memory
    - NVMe SSD tier will be explored in a later milestone
- Technical report on
    - Performance vs cost experiments
    - Analysis on workload patterns
    - general learnings from dealing with SCM
- coded features

### Durability deliverables for system availability and scalability improvements
- Fast restart / Software deployment
- hardware failure
- scaling up/down (partition split/merge)
- Integrate with an existing API for log storage(WAL)
- TSO failover

## Push-down compute support
- Not aligned with industry trends AFAIK
- High contribution value - major feature support
- High research value; many unknowns
- Use cases?
- based on LLVM, LUA, WASM
- shared data schema model
- document-oriented storage
- IR, JIT(Lua), C++ (static pre-compiled)
- API
- Used by SQL/ graph
- stored procedures
- access to read/write records
- ability to route and remote-invoke other instances of compute
- caching for read results

# Additional areas of interest
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
