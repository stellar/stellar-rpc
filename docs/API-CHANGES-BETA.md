# Stellar RPC Archive Node (Beta): API Changes

This guide lists only the differences from the current Stellar RPC. The full method reference is in the [Stellar RPC API docs](https://developers.stellar.org/docs/data/apis/rpc/api-reference/methods). For running the node, see the [operator runbook](ARCHIVE-NODE-BETA-RUNBOOK.md).

## 1. Changes to existing methods

No existing method, parameter, or response field was removed. `getEvents`, `getLedgers`, `getTransaction`, and `getTransactions` now serve full history.

| Method | Change in the archive node |
|---|---|
| `getHealth` | The `ledgerRetentionWindow` field in the response is `0` when the node keeps full history. Use `oldestLedger` to find the oldest served ledger. |
| `getLedgers` | Page limits are maximum 20, default 5. The current RPC uses 200 and 50. A `limit` above 20 returns an error. The operator can change the limits under `[service.methods.getLedgers]`. |

## 2. New method: queryEvents

`queryEvents` is a new method in the archive node. The current RPC does not have it. It follows the proposal in [stellar discussions #1872](https://github.com/orgs/stellar/discussions/1872). Compared with `getEvents`, it adds descending order, positional topic filters, and stateless cursors. The proposal uses the working name `getEventsV2`.

SDK support: the Go SDK has `QueryEvents` from the version pinned in this release. Other SDKs do not support the method yet.

A request is either a new query or a continuation. A new query uses the query fields. A continuation uses `cursor` in place of the query fields.

| Request | Fields |
|---|---|
| New query | `minLedger`, `maxLedger`, `order`, `filters` |
| Continuation | `cursor` |

`limit` and `xdrFormat` are allowed in both. The cursor does not store them. Send them on each request, or the defaults apply.

### Parameters

| Parameter | Type | Status | Meaning |
|---|---|---|---|
| `minLedger` | number | Required for `asc`. Optional for `desc`, where it defaults to 2 | First ledger, inclusive |
| `maxLedger` | number | Optional | Last ledger, inclusive. If omitted: with `asc` the scan has no end and follows new ledgers; with `desc` it starts at the latest ledger |
| `order` | string | Optional, default `asc` | `asc` or `desc` |
| `filters` | object[] | Optional | AND inside a filter, OR across filters. Omit to match all events |
| `cursor` | string | Continuation only | From a previous response. Not allowed with query fields |
| `limit` | number | Optional, default 100 | Events per page, 1 to 1000 |
| `xdrFormat` | string | Optional, default `base64` | `base64` or `json`, for `topic` and `value` in each event |

Filter fields, at least one per filter:

- `type`: `contract` or `system`.
- `contractId`: one contract ID in strkey form (`C...`).
- `topic0` to `topic3`: a base64-encoded XDR `ScVal` that must equal the topic at that position.

Rules:

- `minLedger` above `maxLedger` returns `invalid_params`. `minLedger: 1` is treated as `2`. `minLedger: 0` is the same as omitting it.
- `maxLedger` above the latest ledger is allowed. Ascending serves events up to the latest ledger, then returns `WAITING_FOR_LEDGERS`. Descending returns an empty page with `WAITING_FOR_LEDGERS` until that ledger closes. Ascending with `minLedger` above the latest ledger returns an empty page with `WAITING_FOR_LEDGERS`.
- With `desc` and no `maxLedger`, the top edge is the latest ledger at query start, so every page shares it.
- Descending reverses the full event order, including events inside one ledger.
- An omitted topic position matches any value. A set position must match exactly. There is no topic-count matching, so events with more than four topics still match on positions 0 to 3.
- The list may hold at most 256 filters. Each distinct field value counts against the term budget of 15, however many filters reuse it.
- An empty `filters` list, a `topic4` field, or a `limit` outside 1 to 1000 returns `invalid_params`.

### Result

- `events`: `<object[]>`
  - `type`: `<string>` `contract` or `system`.
  - `ledger`: `<number>` Sequence number of the ledger that holds the event.
  - `ledgerClosedAt`: `<string>` Close time of that ledger, ISO 8601.
  - `contractId`: `<string>` Contract that emitted the event. `""` for events without a contract.
  - `id`: `<string>` Unique event ID. Same format and sort order as in [getEvents](https://developers.stellar.org/docs/data/apis/rpc/api-reference/methods/getEvents).
  - `transactionIndex`: `<number>` Position of the transaction in the ledger, starting at 1.
  - `operationIndex`: `<number>` Position of the operation in the transaction, starting at 0.
  - `txHash`: `<string>` Hash of the transaction.
  - `topic`: `<string[]>` Topics as base64-encoded XDR. Present when `xdrFormat` is `base64`.
  - `value`: `<string>` Event value as base64-encoded XDR. Present when `xdrFormat` is `base64`.
  - `topicJson`: `<object[]>` Topics as JSON. Present when `xdrFormat` is `json`.
  - `valueJson`: `<object>` Event value as JSON. Present when `xdrFormat` is `json`.
- `cursor`: `<string>` Cursor for the next page. Absent only when `scanStatus` is `COMPLETE`.
- `scanStatus`: `<string>` How far the scan got. See the table below.
- `scannedLedger`: `<number>` Last ledger the scan fully covered. In ascending order, every ledger from `minLedger` up to this one is done. In descending order, every ledger from the top edge down to this one is done. A page can end inside a ledger. The cursor then resumes at the next event in that ledger. Before any ledger is covered, the value is `minLedger - 1` ascending or the top edge plus 1 descending.
- `oldestLedger`: `<number>` Oldest ledger the node serves.
- `latestLedger`: `<number>` Latest ledger the node has committed.

| `scanStatus` | Meaning |
|---|---|
| `HAS_MORE` | Scan range remains. Send the cursor. |
| `WAITING_FOR_LEDGERS` | The scan needs ledgers this node does not have yet. Poll with the cursor, about once per ledger close. |
| `OLDEST_REACHED` | The range extends below the oldest ledger this node has. Events below it are not available. Resending the cursor returns the same status. |
| `COMPLETE` | Range fully scanned. No cursor. A descending scan whose `minLedger` is at or above the oldest ledger ends here, not in `OLDEST_REACHED`. |

### Pagination

Every response carries a cursor unless `scanStatus` is `COMPLETE`. A short page does not mean the end of data. One page scans at most 10,000 ledgers. This limit is fixed. Past that, the response is `HAS_MORE` with a cursor, not an error. Always check `scanStatus`. A page can hold zero events. A sparse query over full history needs one request per 10,000 ledgers, so thousands of requests. Put a request cap in your client loop.

The cursor is an opaque string. It holds the query bounds, order, filters, and position, so the server keeps no state between pages. Cursors have no time limit. A future release may retire a cursor format. A retired cursor returns `cursor_malformed`. Then start a new query from the last event's ledger and skip events by `id`. An ascending cursor that points below the oldest served ledger returns `ledger_out_of_range`. A malformed cursor returns `cursor_malformed`.

An ascending query without `maxLedger` never returns `COMPLETE`. It follows the tip and returns `WAITING_FOR_LEDGERS` when it catches up.

### Term budget

Each query may use at most 15 distinct filter terms by default. The operator can change this limit under `[service.methods.queryEvents]`. A term is one distinct field-and-value pair across all filters. `contractId`, `type`, and each set topic position each count. The same contract ID in five filters counts once. The same value in two topic positions counts twice. A query over budget returns `invalid_params` with `termsUsed` and `termBudget` in `error.data`. Split it into narrower queries.

### Errors

Every request rejected for its parameters returns JSON-RPC code `-32602` with `error.data.reason` set to one of:

| `reason` | Extra fields in `error.data` | When |
|---|---|---|
| `invalid_params` | `termsUsed`, `termBudget` (term budget only) | Bad shape, unknown field, bad value, over budget. |
| `ledger_out_of_range` | `missingLedger`, `oldestLedger`, `latestLedger` | An ascending range or cursor position is below the oldest served ledger, or a descending query has `minLedger` above the latest ledger and no `maxLedger`. Descending below the oldest ledger returns `OLDEST_REACHED` instead. |
| `cursor_malformed` | `oldestLedger`, `latestLedger` | The cursor does not parse. |

Unknown request fields are rejected.

getEvents returns `-32600` when the start ledger is out of range. queryEvents returns `-32602` for the same condition.

### Example

Request. All `transfer` and `mint` events from one contract, newest first, two per page. This request uses three terms: `contractId` once, and `topic0` twice. Values in this example are illustrative.

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "queryEvents",
  "params": {
    "maxLedger": 1190000,
    "order": "desc",
    "filters": [
      {
        "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
        "topic0": "AAAADwAAAAh0cmFuc2Zlcg=="
      },
      {
        "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
        "topic0": "AAAADwAAAARtaW50"
      }
    ],
    "limit": 2
  }
}
```

Result:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "events": [
      {
        "type": "contract",
        "ledger": 1189981,
        "ledgerClosedAt": "2024-04-25T17:52:46Z",
        "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
        "id": "0005110929477865472-0000000000",
        "transactionIndex": 1,
        "operationIndex": 0,
        "txHash": "5bd25a1a8fd1fe8ec99ab1fdd57af9a6f0e4c7c8a3c1b7d2e9f0a1b2c3d4e5f6",
        "topic": [
          "AAAADwAAAAh0cmFuc2Zlcg==",
          "AAAAEgAAAAAAAAAAeo0X4ISuIjuArAyYXR0z8qyj0x0MHpv92CTtN4PONek=",
          "AAAAEgAAAAAAAAAAfn3koV5C+Z6q8U1LfPwlofeZ3jnWI6Iudfg5NOEmOtg="
        ],
        "value": "AAAACgAAAAAAAAAAAAAAAAX14QA="
      },
      {
        "type": "contract",
        "ledger": 1189977,
        "ledgerClosedAt": "2024-04-25T17:52:26Z",
        "contractId": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
        "id": "0005110912298004480-0000000000",
        "transactionIndex": 3,
        "operationIndex": 0,
        "txHash": "e1f2a3b4c5d6e7f8091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f70",
        "topic": [
          "AAAADwAAAAh0cmFuc2Zlcg==",
          "AAAAEgAAAAAAAAAAfn3koV5C+Z6q8U1LfPwlofeZ3jnWI6Iudfg5NOEmOtg=",
          "AAAAEgAAAAAAAAAAeo0X4ISuIjuArAyYXR0z8qyj0x0MHpv92CTtN4PONek="
        ],
        "value": "AAAACgAAAAAAAAAAAAAAAACYloA="
      }
    ],
    "cursor": "gec1_AQEAAAASJnIAAAASJm0AAAAB...",
    "scanStatus": "HAS_MORE",
    "scannedLedger": 1189978,
    "oldestLedger": 2,
    "latestLedger": 1190004
  }
}
```

Next page:

```json
{
  "jsonrpc": "2.0",
  "id": 2,
  "method": "queryEvents",
  "params": {
    "cursor": "gec1_AQEAAAASJnIAAAASJm0AAAAB...",
    "limit": 2
  }
}
```

### Limitations

- Diagnostic events are not stored. Only contract and system events are served, the same as the current RPC.
