# stellar-rpc
RPC Server for Stellar.


This repo is home to the Stellar RPC. The RPC provides information that the network currently has in its view. It has the ability to send a transaction to the network, and query the network for the status of previously sent transactions, and is meant to be simple, scalable, and familiar to blockchain developers.


## RPC Methods
To learn about the RPC methods, please see our [RPC Developer Docs](https://developers.stellar.org/docs/data/apis/rpc/api-reference/methods).

## To Use an Ecosystem RPCs
To use RPC from an ecosystem provider for futurenet, testnet, or mainnet, please see our list of [Ecosystem RPC Providers](https://developers.stellar.org/docs/data/apis/api-providers).

## Run Your Own RPC
If you are interested in running your own RPC, please review the [Admin Guide](https://developers.stellar.org/docs/data/apis/rpc/admin-guide).

## Run Tests

Unit tests:

```bash
go test -v -failfast ./...
```

Integration tests:

```bash
STELLAR_RPC_INTEGRATION_TESTS_ENABLED=true \
STELLAR_RPC_INTEGRATION_TESTS_CORE_MAX_SUPPORTED_PROTOCOL=23 \
STELLAR_RPC_INTEGRATION_TESTS_CAPTIVE_CORE_BIN=$(which stellar-core) \
    go test -v -failfast ./cmd/stellar-rpc/internal/integrationtest/...
```

## Latest Release
For latest releases, please see
[releases](https://github.com/stellar/stellar-rpc/releases).

## Upcoming Features
For upcoming features, please see the [project board](https://github.com/orgs/stellar/projects/37/views/29).

## Report Bugs or Request Features
To report bugs or request features, please open an issue on the official [RPC repo](https://github.com/stellar/stellar-rpc/issues/new).

## To Contribute
Please fork this see `good first issues` on [here](https://github.com/stellar/stellar-rpc/contribute).

Developer Docs: https://developers.stellar.org/docs
