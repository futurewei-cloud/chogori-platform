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

#include <k2/transport/PayloadSerialization.h>
#include <seastar/core/sharded.hh>
#include <k2/transport/Payload.h>
#include <k2/transport/Status.h>
#include <k2/dto/Persistence.h>
#include <k2/common/Common.h>
#include <k2/config/Config.h>
#include <k2/cpo/client/CPOClient.h>
#include <k2/transport/BaseTypes.h>
#include <k2/transport/TXEndpoint.h>
#include <k2/appbase/AppEssentials.h>
#include <k2/appbase/Appbase.h>


namespace k2 {

class PlogClient {
public:
    PlogClient();
    ~PlogClient();

    // obtain the persistence cluster for a given name from the cpo and obtain the endpoints of the plog server
    seastar::future<> init(String clusterName);

    // allow users to select a specific persistence group by its name
    bool selectPersistenceGroup(String name);

    // create a plog with retry times
    // TODO: revise this method, making this retry as an internal config variable instead of parameter. 
    seastar::future<std::tuple<Status, String>> create(uint8_t retries = 1);

    // append a payload into a plog at the given offset 
    seastar::future<std::tuple<Status, uint32_t>> append(String plogId, uint32_t offset, Payload payload);

    // read a payload 
    seastar::future<std::tuple<Status, Payload>> read(String plogId, uint32_t offset, uint32_t size);

    // seal a payload
    seastar::future<std::tuple<Status, uint32_t>> seal(String plogId, uint32_t offset);

private:
    dto::PersistenceCluster _persistenceCluster; // the current persistence cluster the client holds
    std::unordered_map<String, std::vector<std::unique_ptr<TXEndpoint>>> _persistenceMapEndpoints; // the map of persistence group name and plog server endpoints
    std::unordered_map<String, uint32_t> _persistenceNameMap; // key - name, value - the index of the name in _persistenceNameList
    std::vector<String> _persistenceNameList; // a list to store all the names of persistence groups in current persistence cluster
    uint32_t _persistenceMapPointer; // indicate the current used persistence group

    CPOClient _cpo;

    // generate the plog id
    // TODO: change the method to generate the random plog id later
    String _generatePlogId();

    // obtain the endpoints of the plog server
    seastar::future<> _getPlogServerEndpoints(); 

    // obtain the persistence cluster for a given name from the cpo
    seastar::future<> _getPersistenceCluster(String clusterName); 

    ConfigDuration _cpo_timeout {"cpo_timeout", 1s};
    ConfigDuration _plog_timeout{"plog_timeout", 100ms};
    ConfigVar<String> _cpo_url{"cpo_url", ""};

};

} // k2
