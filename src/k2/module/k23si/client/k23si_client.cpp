//<!--
//    (C)opyright Futurewei Technologies Inc, 2020
//-->

#include "k23si_client.h"

namespace k2 {

seastar::future<EndResult> K2TxnHandle::end(bool shouldCommit) {
    (void) shouldCommit; return seastar::make_ready_future<EndResult>(EndResult(Status::S200_OK())); 
}

K23SIClient::K23SIClient(const K23SIClientConfig &, const std::vector<std::string>& _endpoints, std::string _cpo) {
    for (auto it = _endpoints.begin(); it != _endpoints.end(); ++it) {
        _k2endpoints.push_back(String(*it));
    }
    _cpo_client = CPOClient(String(_cpo));
}

seastar::future<Status> K23SIClient::makeCollection(const String& collection) {
    std::vector<String> endpoints = _k2endpoints;

    dto::CollectionMetadata metadata{
        .name = collection,
        .hashScheme = dto::HashScheme::HashCRC32C,
        .storageDriver = dto::StorageDriver::K23SI,
        .capacity = {},
        .retentionPeriod = Duration(600s)
    };

    return _cpo_client.CreateAndWaitForCollection(Deadline<>(5s), std::move(metadata), std::move(endpoints));
}

seastar::future<K2TxnHandle> K23SIClient::beginTxn(const K2TxnOptions& options) {
    return seastar::make_ready_future<K2TxnHandle>(K2TxnHandle(options, &_cpo_client));
};

} // namespace k2
