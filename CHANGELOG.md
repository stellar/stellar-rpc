# Changelog

## Unreleased


## [v25.0.0](https://github.com/stellar/stellar-rpc/compare/v24.0.0...v25.0.0): Protocol 25

### Breaking Changes
* The RPC client and schema definitions have been moved to the unified Stellar Go SDK. Please install them from there, instead:

```bash
go get -u github.com/stellar/go-stellar-sdk/clients/rpcclient
go get -u github.com/stellar/go-stellar-sdk/protocols/rpc
```

### Added
- Added `--backfill` configuration parameter providing synchronous backfilling of `HISTORY_RETENTION_WINDOW` ledgers to the local DB prior to RPC starting ([#571](https://github.com/stellar/stellar-rpc/pull/571)).
- Expanded `getLatestLedger` endpoint to also return `closeTime`, `headerXdr`, and `metadataXdr` ([#554](https://github.com/stellar/stellar-rpc/pull/554)).
- Added `soroban-env-host` info to `version` command ([#550](https://github.com/stellar/stellar-rpc/pull/550)).
- Added `--network` configuration parameter, allowing users to specify a default Stellar network (`testnet`, `pubnet`, or `futurenet`) ([#540](https://github.com/stellar/stellar-rpc/pull/540), [#543](https://github.com/stellar/stellar-rpc/pull/543)).
- Simulation has been updated to support Protocol 25 ([#548](https://github.com/stellar/stellar-rpc/pull/548)).

### Fixed
- `version` command now works consistently on MacOS ([#550](https://github.com/stellar/stellar-rpc/pull/550)).
- JSON-RPC methods now accept `params: {}` ([#551](https://github.com/stellar/stellar-rpc/issues/551)).


## [v24.0.0](https://github.com/stellar/stellar-rpc/compare/v23.0.4...v24.0.0): Protocol 24

### Breaking Changes
* **Support for Protocol 24**.
* Simulation will only work for transactions built for >= Protocol 23 ([#522](https://github.com/stellar/stellar-rpc/pull/522)).


## [v23.0.4](https://github.com/stellar/stellar-rpc/compare/v23.0.3...v23.0.4)

### Fixed
- The simulation library has been upgraded with bug fixes ([#515](https://github.com/stellar/stellar-rpc/pull/515)).


## [v23.0.3](https://github.com/stellar/stellar-rpc/compare/v23.0.2...v23.0.3)

**This release includes a database migration.** This migration may take up to an hour to build new indices with a 7-day retention window; this will depending on your hardware, so please deploy accordingly.

### Added (`rpcclient`)
- The RPC's `Client` now has a new method `LoadAccount` which returns an SDK-compatible `Account` interface for a public key ([#481](https://github.com/stellar/stellar-rpc/pull/481/)).

### Fixed
- `getLedgers` performance has been improved ([#505](https://github.com/stellar/stellar-rpc/pull/505)).
- `getEvents` performance has been drastically improved ([#510](https://github.com/stellar/stellar-rpc/pull/510)).


## [v23.0.2](https://github.com/stellar/stellar-rpc/compare/v23.0.1...v23.0.2)

### Fixed
- `simulateTransaction` would occasionally emit state changes that did not have a `type` ([#506](https://github.com/stellar/stellar-rpc/pull/506)).


## [v23.0.1](https://github.com/stellar/stellar-rpc/compare/v23.0.0...v23.0.1)
- Bump soroban-env lib to v23.0.1 ([#499](https://github.com/stellar/stellar-rpc/pull/499)).


## [v23.0.0](https://github.com/stellar/stellar-rpc/compare/v22.1.5...v23.0.0): Protocol 23 Release

### Breaking Changes
- **Support for Protocol 23,** notably `TransactionMetaV4` and `LedgerCloseMetaV2`, see [`stellar-xdr @ v23.0`](https://github.com/stellar/stellar-xdr/tree/v23.0) for the full protocol schema.
- The `getLedgerEntry` endpoint has been removed. This endpoint was already deprecated earlier in favor of `getLedgerEntries` and is completely removed in this release.
- The `pagingToken` field of `getEvents` results has been removed, use the `id` field for individual events or `cursor` at the top level for pagination ([#382](https://github.com/stellar/stellar-rpc/pull/382)).
- The `snake_case`d fields of `getVersionInfo` have been removed (`commit_hash`, etc.); prefer the `camelCase`d versions ([#382](https://github.com/stellar/stellar-rpc/pull/382)).
- Diagnostic events will **no longer be present** in the `getEvents` stream ([#4590](https://github.com/stellar/stellar-rpc/pull/4590)).

### Deprecations
- The `inSuccessfulContractCall` field of `getEvents` is now deprecated and will be removed in the next version ([#4590](https://github.com/stellar/stellar-rpc/pull/4590)).

### Added
- You can now use an external datastore as a source for `getLedgers` ([#437](https://github.com/stellar/stellar-rpc/pull/437)).
- Transactions that have expired footprints will now auto-restore in their simulation result ([#463](https://github.com/stellar/stellar-rpc/pull/463)).
- Added a top-level `"events"` structure to the `getTransaction` and `getTransactions` endpoint which breaks down events into disjoint `contractEvents[Xdr|Json]` and `transactionEvents[Xdr|Json]` ([#455](https://github.com/stellar/stellar-rpc/pull/455)).
- Added `"**"` wildcard to the `getEvents` endpoint, enabling flexible topic matching without manual padding. For example, `["X", "**"]` filter matches events with `"X"` as the first topic followed by any number of topics. The wildcard can be used only as the last or only topic ([#419](https://github.com/stellar/stellar-rpc/pull/419)).
- Added a field to `getLedgerEntries` results, the `extension[Xdr|Json]` field representing the `LedgerEntry`'s extension ([#388](https://github.com/stellar/stellar-rpc/pull/388)).
- Added support for non-root authorization to `simulateTransaction` with a new, optional parameter `authMode` which can be `enforce`, `record`, and `record_allow_nonroot` ([#432](https://github.com/stellar/stellar-rpc/pull/432)).
- `getEvents` now includes an `opIndex` for each event ([#383](https://github.com/stellar/stellar-rpc/pull/383)).
- Added missing ledger range fields to `getEvents`, namely `oldestLedger`, `latestLedgerCloseTime`, and `oldestLedgerCloseTime` to correspond to all other endpoints ([#409](https://github.com/stellar/stellar-rpc/pull/409)).
- `getLedgerEntries` now uses RPC's internal Captive Core's high-performance HTTP server rather than storing entries locally in sqlite ([#353](https://github.com/stellar/stellar-rpc/pull/353)).

### Fixed
- Event topic filters can now serialize and deserialize correctly ([#427](https://github.com/stellar/stellar-rpc/pull/427), [#449](https://github.com/stellar/stellar-rpc/pull/449)).
- Fixed a potential scenario where `getLedgers` would crash with invalid parameters ([#407](https://github.com/stellar/stellar-rpc/pull/407)).
- Various scenarios where memory could leak have been fixed ([#474](https://github.com/stellar/stellar-rpc/pull/474), [#472](https://github.com/stellar/stellar-rpc/pull/472)).
- The simulation library behind `simulateTransaction` has been updated to Protocol 23 ([#484](https://github.com/stellar/stellar-rpc/pull/484)).


## [v23.0.0-rc.2](https://github.com/stellar/stellar-rpc/compare/v22.1.4...v23.0.0-rc.2)

### Breaking Changes
- The new top-level `"events"` structure to the `getTransaction` and `getTransactions` endpoint no longer has the `diagnosticEvents[Xdr|Json]`; prefer the top-level field instead as it will contain *all* of the diagnostic events that occurred in a transaction ([#455](https://github.com/stellar/stellar-rpc/pull/455)).

### Added
- Auto-restoration for transactions that have expired footprints ([#463](https://github.com/stellar/stellar-rpc/pull/463)).


## [v23.0.0-rc.1](https://github.com/stellar/stellar-rpc/compare/v22.1.3...v23.0.0-rc.1)

### Breaking Changes
- **Support for Protocol 23.**
- The `getLedgerEntry` endpoint has been removed. This endpoint was already deprecated earlier in favor of `getLedgerEntries` and is completely removed in this release.
- Diagnostic events will **no longer be present** in the `getEvents` stream ([#4590](https://github.com/stellar/stellar-rpc/pull/4590)).
- The `inSuccessfulContractCall` field of `getEvents` is now deprecated and will be removed in the next version ([#4590](https://github.com/stellar/stellar-rpc/pull/4590)).

### Added
- Added a top-level `"events"` structure to the `getTransaction` and `getTransactions` endpoint which breaks down events into disjoint `diagnosticEvents[Xdr|Json]`, `contractEvents[Xdr|Json]`, and `transactionEvents[Xdr|Json]` ([#455](https://github.com/stellar/stellar-rpc/pull/455)).
- Added `"**"` wildcard to the `getEvents` endpoint, enabling flexible topic matching without manual padding. For example, `["X", "**"]` filter matches events with `"X"` as the first topic followed by any number of topics. The wildcard can be used only as the last or the only topic ([#419](https://github.com/stellar/stellar-rpc/pull/419)).
- Added a field to `getLedgerEntries` results, the `extension[Xdr|Json]` field representing the `LedgerEntry`'s extension ([#388](https://github.com/stellar/stellar-rpc/pull/388)).
- Added support for non-root authorization to `simulateTransaction` with a new, optional parameter `authMode` which can be `enforce`, `record`, and `record_allow_nonroot` ([#432](https://github.com/stellar/stellar-rpc/pull/432)).
- `getEvents` now includes an `opIndex` for each event ([#383](https://github.com/stellar/stellar-rpc/pull/383)).
- Added missing ledger range fields to `getEvents`, namely `oldestLedger`, `latestLedgerCloseTime`, and `oldestLedgerCloseTime` to correspond to all other endpoints ([#409](https://github.com/stellar/stellar-rpc/pull/409)).

### Fixed
- Event topic filters can now serialize and deserialize correctly ([#427](https://github.com/stellar/stellar-rpc/pull/427), [#449](https://github.com/stellar/stellar-rpc/pull/449)).
- Fixed a potential scenario where `getLedgers` would crash with invalid parameters ([#407](https://github.com/stellar/stellar-rpc/pull/407)).


## [v21.5.1](https://github.com/stellar/stellar-rpc/compare/v21.5.0...v21.5.1)

### Fixed
* Preserve field omission behavior of `simulateTransaction` ([#291](https://github.com/stellar/stellar-rpc/pull/291)).

## [v21.5.0](https://github.com/stellar/stellar-rpc/compare/v21.4.1...v21.5.0)

### Added

- Add `Cursor` in `GetEventsResponse`. This tells the client until what ledger events are being queried. e.g.: `startLEdger` (inclusive) - `endLedger` (exclusive)
- Limitation: getEvents are capped by 10K `LedgerScanLimit` which means you can query events for 10K ledger at maximum for a given request.
- Add `EndLedger` in `GetEventsRequest`. This provides finer control and clarity on the range of ledgers being queried.
- Disk-Based Event Storage: Events are now stored on disk instead of in memory. For context, storing approximately 3 million events will require around 1.5 GB of disk space.
This change enhances the scalability and can now support a larger retention window (~7 days) for events.
- Ledger Scanning Limitation: The getEvents RPC will now scan a maximum of `10,000` ledgers per request. This limits the resource usage and ensures more predictable performance, especially for queries spanning large ledger ranges.
- A migration process has been introduced to transition event storage from in-memory to disk-based storage.

* Add support for unpacked JSON responses of base64-encoded XDR fields via a new, optional parameter. When omitted, the behavior does not change and we encode fields as base64.
```typescript
xdrFormat?: "" | "base64" | "json"
```
  - `getTransaction`
  - `getTransactions`
  - `getLedgerEntry`
  - `getLedgerEntries`
  - `getEvents`
  - `sendTransaction`
  - `simulateTransaction`

There are new field names for the JSONified versions of XDR structures. Any field with an `Xdr` suffix (e.g., `resultXdr` in `getTransaction()`) will be replaced with one that has a `Json` suffix (e.g., `resultJson`) that is a JSON object verbosely and completely describing the XDR structure.

Certain XDR-encoded fields do not have an `Xdr` suffix, but those also have a `*Json` equivalent and are listed below:
  * _getEvents_: `topic` -> `topicJson`, `value` -> `valueJson`
  * _getLedgerEntries_: `key` -> `keyJson`, `xdr` -> `dataJson`
  * _getLedgerEntry_: `xdr` -> `entryJson`
  * _simulateTransaction_: `transactionData`, `events`, `results.auth`,
    `restorePreamble.transactionData`, `stateChanges.key|before|after` all have a
    `Json` suffix, and `results.xdr` is now `results.returnValueJson`

### Fixed
* Improve performance of `getVersionInfo` and `getNetwork` ([#198](https://github.com/stellar/stellar-rpc/pull/198)).


## [v21.4.1](https://github.com/stellar/stellar-rpc/compare/v21.4.0...v21.4.1)

### Fixed
* Fix parsing of the `--log-format` parameter ([#252](https://github.com/stellar/stellar-rpc/pull/252))


## [v21.4.0](https://github.com/stellar/stellar-rpc/compare/v21.2.0...v21.4.0)

### Added
* Transactions will now be stored in a database rather than in memory ([#174](https://github.com/stellar/stellar-rpc/pull/174)).

You can opt-in to longer transaction retention by setting `--transaction-retention-window` / `TRANSACTION_RETENTION_WINDOW` to a higher number of ledgers. This will also retain corresponding number of ledgers in the database. Keep in mind, of course, that this will cause an increase in disk usage for the growing database.

* Unify transaction and event retention windows ([#234](https://github.com/stellar/stellar-rpc/pull/234)).
* There is a new `getTransactions` endpoint with the following API ([#136](https://github.com/stellar/stellar-rpc/pull/136)):

```typescript
interface Request {
  startLedger: number; // uint32
  pagination?: {
    cursor?: string;
    limit?:  number; // uint
  }
}

interface Response {
  transactions: Transaction[];        // see below
  latestLedger: number;               // uint32
  latestLedgerCloseTimestamp: number; // int64
  oldestLedger: number;               // uint32
  oldestLedgerCloseTimestamp: number; // int64
  cursor: string;
}

interface Transaction {
  status: boolean;          // whether or not the transaction succeeded
  applicationOrder: number; // int32, index of the transaction in the ledger
  feeBump: boolean;         // if it's a fee-bump transaction
  envelopeXdr: string;      // TransactionEnvelope XDR
  resultXdr: string;        // TransactionResult XDR
  resultMetaXdr: string;    // TransactionMeta XDR
  ledger: number;           // uint32, ledger sequence with this transaction
  createdAt: int64;         // int64, UNIX timestamp the transaction's inclusion
  diagnosticEventsXdr?: string[]; // if failed, DiagnosticEvent XDRs
}
```

### Fixed
* Logging and typo fixes in ([#238](https://github.com/stellar/stellar-rpc/pull/238)).
* Fix calculation of ledger ranges across endpoints ([#217](https://github.com/stellar/stellar-rpc/pull/217)).


## [v21.2.0](https://github.com/stellar/stellar-rpc/compare/v21.1.0...v21.2.0)

### Added
* Dependencies have been updated (`stellar/go`) to enable `ENABLE_DIAGNOSTICS_FOR_TX_SUBMISSION` by default ([#179](https://github.com/stellar/stellar-rpc/pull/179)).

### Fixed
* The Captive Core path is supplied correctly for TOML generation ([#178](https://github.com/stellar/stellar-rpc/pull/178)).


## [v21.1.0](https://github.com/stellar/stellar-rpc/compare/v21.0.1...v21.1.0)

### Added
* A new `getVersionInfo` RPC endpoint providing versioning info ([#132](https://github.com/stellar/stellar-rpc/pull/132)):

```typescript
interface getVersionInfo {
  version: string;
  commit_hash: string;
  build_time_stamp: string;
  captive_core_version: string;
  protocol_version: number; // uint32
}
```

### Fixed
* Deadlock on events ingestion error ([#167](https://github.com/stellar/stellar-rpc/pull/167)).
* Correctly report row iteration errors in `StreamAllLedgers` ([#168](https://github.com/stellar/stellar-rpc/pull/168)).
* Increase default ingestion timeout ([#169](https://github.com/stellar/stellar-rpc/pull/169)).
* Surface an ignored error in `getRawLedgerEntries()` ([#170](https://github.com/stellar/stellar-rpc/pull/170)).


# Formatting Guidelines

This outlines the formatting expectations for the CHANGELOG.md file.

## [vMajor.Minor.Patch](GitHub compare diff to last version)
If necessary, drop a summary here (e.g. "This release supports Protocol 420.")

### Breaking Changes come first
* This is a pull request description and it should be quite detailed if it's a breaking change. Ideally, you would even include a bit of helpful notes on how to migrate/upgrade if that's necessary. For example, if an API changes, you should provide deep detail on the delta.

### Added stuff next
* Anything added should have a details on its schema or command line arguments ([#NNNN](link to github pr)).

### Fixed bugs last
* Be sure you describe who is affected and how.
