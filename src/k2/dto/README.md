This folder describes the wire interfaces for K2, and provides the types (DTOs) needed to send requests and receive
responses when dealing with K2.

The reason why we have all of these interfaces in one place is to facilitate mutual dependency on the types. For example, the ControlPlaneOracle will send ASSIGN message to K2 nodes in order to assign partitions. K2 nodes on the other hand will call ControlPlaneOracle to discover new/changed Collections (e.g. needed for the PUSH operation in K2-3SI transactions)

TODO: For now we keep all interfaces here. We should consider refactoring into internal/external
