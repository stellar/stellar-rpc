// Package txquery resolves getTransaction(hash) end-to-end against
// the full-history cold tier.
//
// The chain is: txhash MPHF -> ledgerSeq -> cold ledger pack ->
// LedgerCloseMeta -> ingest reader -> db.Transaction. The output
// shape matches what methods.GetTransaction consumes from the hot
// DB path, so a ColdLookup can be dropped into the RPC handler as
// a db.TransactionReader.
package txquery

import (
	"context"
	"errors"
	"fmt"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/txhash"
)

// ColdLookup serves getTransaction(hash) from the cold tier.
//
// Concurrency: GetTransaction is safe for concurrent use as long as
// the underlying readers stay open. Closing either reader concurrent
// with an in-flight GetTransaction is undefined; the caller owns
// reader lifecycle.
//
// Fee bumps: the cold MPHF seeder indexes only the outer hash
// (LedgerCloseMeta.TransactionHash), so inner-hash lookups return
// ErrNoTransaction even though the hot DB path resolves them. Fix
// is a seeder change, not a lookup change.
type ColdLookup struct {
	tx         *txhash.ColdReader
	ledgers    *ledger.ColdStoreReader
	passphrase string
}

// Compile-time confirmation that ColdLookup satisfies the same
// interface as the hot DB path, so it can be dropped into the
// methods.GetTransaction handler as a TransactionReader.
var _ db.TransactionReader = (*ColdLookup)(nil)

// NewColdLookup wires a txhash MPHF reader and a cold ledger pack
// reader together. passphrase must match the network the ledgers
// were closed against — it's what HashTransactionInEnvelope used at
// ingest time, and ingest.NewLedgerTransactionReaderFromLedgerCloseMeta
// uses it to recompute hashes when walking the LCM.
func NewColdLookup(
	tx *txhash.ColdReader,
	ledgers *ledger.ColdStoreReader,
	passphrase string,
) *ColdLookup {
	return &ColdLookup{tx: tx, ledgers: ledgers, passphrase: passphrase}
}

// GetTransaction implements db.TransactionReader. ctx satisfies the
// interface; the cold path is mmap-backed and performs no
// context-aware I/O.
//
// Returns db.ErrNoTransaction for both a true miss (hash not in the
// MPHF) and a residual MPHF false positive (~1/256 of unseen-key
// probes survive the 1-byte fingerprint, then fail the in-LCM scan).
// Other I/O or decode failures bubble up wrapped.
func (c *ColdLookup) GetTransaction(_ context.Context, hash xdr.Hash) (db.Transaction, error) {
	seq, err := c.tx.Lookup([32]byte(hash))
	if err != nil {
		if errors.Is(err, stores.ErrNotFound) {
			return db.Transaction{}, db.ErrNoTransaction
		}
		return db.Transaction{}, fmt.Errorf("txhash lookup: %w", err)
	}

	raw, err := c.ledgers.GetLedgerRaw(seq)
	if err != nil {
		if errors.Is(err, stores.ErrNotFound) {
			// MPHF pointed outside this pack's window — almost
			// certainly a fingerprint-survived false positive on an
			// unseen hash.
			return db.Transaction{}, db.ErrNoTransaction
		}
		return db.Transaction{}, fmt.Errorf("ledger read seq=%d: %w", seq, err)
	}

	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(raw); err != nil {
		return db.Transaction{}, fmt.Errorf("unmarshal LCM seq=%d: %w", seq, err)
	}

	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(c.passphrase, lcm)
	if err != nil {
		return db.Transaction{}, fmt.Errorf("ingest reader seq=%d: %w", seq, err)
	}

	// Linear scan by hash. The hot DB stores application_order
	// alongside the hash so it can Seek directly; the cold MPHF
	// payload is just the ledgerSeq, so we walk the LCM to find
	// the matching tx. With Stellar's typical txs/ledger this is
	// cheap relative to the pack read above.
	n := lcm.CountTransactions()
	for i := range n {
		ingestTx, rerr := reader.Read()
		if rerr != nil {
			return db.Transaction{}, fmt.Errorf("ingest read seq=%d i=%d: %w", seq, i, rerr)
		}
		if ingestTx.Result.TransactionHash == hash {
			return db.ParseTransaction(lcm, ingestTx)
		}
	}
	return db.Transaction{}, db.ErrNoTransaction
}
