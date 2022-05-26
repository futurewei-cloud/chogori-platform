/*
MIT License

Copyright(c) 2020 Futurewei Cloud

    Permission is hereby granted,
    free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

    The above copyright notice and this permission notice shall be included in all copies
    or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS",
    WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER
    LIABILITY,
    WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
*/

#pragma once
#include <k2/transport/RPCTypes.h>

namespace k2 {
namespace dto {
// These are the knowns verbs in K2. Since verbs are just ints for performance reasons, to make sure we do not
// have a mismatch between different builds, we keep all the verbs in one place
enum Verbs : k2::Verb {
    /************ CPO *****************/
    // ControlPlaneOracle: asked to create a collection
    CPO_COLLECTION_CREATE = 20,
    // ControlPlaneOracle: asked to return an existing collection
    CPO_COLLECTION_GET,
    // ControlPlaneOracle: asked to register a persistence server
    CPO_PERSISTENCE_CLUSTER_CREATE,
    // ControlPlaneOracle: asked to return a collection of persistence servers
    CPO_PERSISTENCE_CLUSTER_GET,
    CPO_PARTITION_METADATA_PUT,
    CPO_PARTITION_METADATA_GET,

    CPO_SCHEMA_CREATE,
    CPO_SCHEMAS_GET,
    CPO_COLLECTION_DROP,

    CPO_HEARTBEAT,
    CPO_GET_TSO_ENDPOINTS,
    CPO_GET_PERSISTENCE_ENDPOINTS,

    /************ Assignment *****************/
    // K2Assignment: CPO asks K2 to assign a partition
    K2_ASSIGNMENT_CREATE = 40,
    // K2Assignment: CPO asks K2 to offload a partition
    K2_ASSIGNMENT_OFFLOAD,

    /************ K23SI *****************/
    // K23SI reads
    K23SI_READ = 60,
    // K23SI writes
    K23SI_WRITE,
    // K23SI push operation
    K23SI_TXN_PUSH,
    // K23SI end transaction
    K23SI_TXN_END,
    // K23SI heartbeat transaction
    K23SI_TXN_HEARTBEAT,
    // sent to finalize a K23SI write
    K23SI_TXN_FINALIZE,
    K23SI_PUSH_SCHEMA,
    K23SI_QUERY,

    /************ K23SI Persistence *****************/
    K23SI_Persist = 80,
    PLOG_CREATE,
    PLOG_APPEND,
    PLOG_READ,
    PLOG_SEAL,
    PLOG_GET_STATUS,
    /************ K23SI Inspection ******************/
    K23SI_INSPECT_RECORDS = 100,
    K23SI_INSPECT_TXN,
    K23SI_INSPECT_WIS,
    K23SI_INSPECT_ALL_TXNS,
    K23SI_INSPECT_ALL_KEYS,

    /************* TSO *******************/
    // API from TSO client to TSO server to get URLs of its nodes (i.e. service end points at worker CPU core)
    GET_TSO_SERVICE_NODE_URLS = 120,
    // API from TSO client to get timestamp from any TSO worker core
    GET_TSO_TIMESTAMP,
    GET_TSO_ASSIGN,
    /************ END OF RESERVED BLOCK *****************/
    END=200
};

} // namespace dto
} // namespace k2
