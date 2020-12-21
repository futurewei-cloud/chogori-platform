This folder describes the wire interfaces for K2, and provides the types (DTOs) needed to send requests and receive
responses when dealing with K2.

The reason why we have all of these interfaces in one place is to facilitate mutual dependency on the types. For example, the ControlPlaneOracle will send ASSIGN message to K2 nodes in order to assign partitions. K2 nodes on the other hand will call ControlPlaneOracle to discover new/changed Collections (e.g. needed for the PUSH operation in K2-3SI transactions)

- Each component which wants to expose a DTO interface, should create a separate file here with its DTOs
- The verbs used to access the interface should be added to the common verbs in MessageVerbs.h
- For objects used in the JSON API HTTP server (see infrastructure/APIServer.h), to and from json methods need to be added. They are not needed for K2 RPCs so can otherwise be omitted.

TODO: For now we keep all interfaces here. We should consider refactoring into internal/external
