// Package views holds zero-copy XDR-view extractors that turn a raw
// LedgerCloseMeta (wire bytes, wrapped as xdr.LedgerCloseMetaView) into
// the per-type shapes the full-history ingestion pipeline writes —
// without ever calling lcm.UnmarshalBinary or any per-element
// MarshalBinary. Each extractor walks the SDK's generated view
// accessors (xdr.LedgerCloseMetaView and friends), which slice directly
// into the input buffer, and copies out only the small scalars it
// needs.
//
// These extractors build rpc-side shapes (events.Payload,
// txhash.Entry, the package-local Transaction), so they live here in
// the rpc tree rather than in the SDK. The SDK already provides the
// zero-copy view navigation types (xdr.LedgerCloseMetaView is a []byte
// alias); this package composes them into product-specific extraction.
// Nothing in this package belongs in the SDK. The read-path extractors
// deliberately return a package-local Transaction rather than
// internal/db.Transaction so views stays a clean leaf (no inversion of
// the db → views layering).
//
// Four extractors are provided:
//
//   - ExtractEvents — ingestion: one events.Payload per emitted
//     contract event (V1/V2 LCM only).
//   - ExtractTxHashes — ingestion: every tx hash in apply order
//     (V0/V1/V2 LCM).
//   - ExtractTxDetailsByHash — read path (getTransaction): the
//     materialized Transaction for one hash (V0/V1/V2 LCM).
//   - ExtractTransactions — read path (paginated getTransactions): a
//     page of Transactions in apply order from a start index
//     (V0/V1/V2 LCM).
//
// Buffer-lifetime contract: extractors that return slices aliasing the
// view buffer document it on the function (e.g. ExtractEvents's
// ContractEventBytes; the raw Envelope/Result/Meta/event fields of
// Transaction). ExtractTxHashes copies its hashes, so its result is
// independent of the view buffer.
//
// Trusted-input invariant (TransactionMeta V3): stellar-core only attaches
// SorobanMeta to Soroban transactions, so "SorobanMeta present ⟺ soroban
// tx" holds for any LCM core emits. ExtractEvents relies on this invariant
// directly — it has no envelope in hand and emits V3 SorobanMeta.Events
// whenever SorobanMeta is present. The read-path extractors pair each tx
// with its envelope anyway (for the Envelope field), so they additionally
// gate V3 contract events on the envelope's soroban-ness
// (gateV3ContractEvents), matching the struct path's IsSorobanTx check in
// GetTransactionEvents. On an LCM that violates the invariant the events
// index and the transaction read path would disagree about the same
// transaction; such input is outside this package's contract.
package views
