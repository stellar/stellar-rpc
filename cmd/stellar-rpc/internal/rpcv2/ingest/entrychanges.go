package ingest

import (
	"context"
	"fmt"
	"os"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/entrychange"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
)

// entryChangesCold builds a chunk's ledger entry change index: the keys each
// ledger changes, accumulated in memory and written at finalize. It emits no
// ingest metrics of its own.
type entryChangesCold struct {
	writer *entrychange.ColdWriter
	failed bool
}

func newEntryChangesCold(dir string, chunkID chunk.ID, secret [stores.SecretLen]byte) (*entryChangesCold, error) {
	// WriteColdIndexFiles creates no directory of its own.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	return &entryChangesCold{writer: entrychange.NewColdWriter(chunkID, dir, secret)}, nil
}

// write records the keys one ledger changes. It decodes the whole ledger: the
// change reader works on decoded meta, not the view the other writers share.
func (e *entryChangesCold) write(seq uint32, lcmView xdr.LedgerCloseMetaView) error {
	var lcm xdr.LedgerCloseMeta
	if err := lcm.UnmarshalBinary(lcmView); err != nil {
		e.failed = true
		return fmt.Errorf("decode ledger seq %d: %w", seq, err)
	}
	keys, err := entrychange.ChangedKeys(lcm)
	if err != nil {
		e.failed = true
		return fmt.Errorf("ledger entry changes seq %d: %w", seq, err)
	}
	terms := make([]event.TermKey, len(keys))
	for i, k := range keys {
		terms[i] = entrychange.TermKey(k)
	}
	e.writer.Add(seq, terms)
	return nil
}

func (e *entryChangesCold) finalize(ctx context.Context) error {
	if e.failed {
		return fmt.Errorf("entry changes cold writer: finalize after failed write")
	}
	if err := e.writer.Finish(ctx); err != nil {
		return fmt.Errorf("entry changes index: %w", err)
	}
	return nil
}
