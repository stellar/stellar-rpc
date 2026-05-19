package main

import (
	"fmt"

	supportlog "github.com/stellar/go-stellar-sdk/support/log"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/ledger"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

// ledgerReader is the common surface both HotStore and ColdStoreReader
// expose for point lookups. Kept here because bench_tx_page.go still
// uses the --tier=hot|cold split.
type ledgerReader interface {
	GetLedgerRaw(seq uint32) ([]byte, error)
	Close() error
}

// rangeReader extends ledgerReader with the iterator used for range scans.
type rangeReader interface {
	ledgerReader
	iterateRange(start, end uint32, fn func(seq uint32, bytes []byte) bool) error
}

// hotAdapter wraps *ledger.HotStore as a rangeReader.
type hotAdapter struct{ *ledger.HotStore }

func (h hotAdapter) iterateRange(start, end uint32, fn func(uint32, []byte) bool) error {
	for entry, err := range h.IterateLedgers(start, end) {
		if err != nil {
			return err
		}
		if !fn(entry.Seq, entry.Bytes) {
			return nil
		}
	}
	return nil
}

// coldAdapter wraps *ledger.ColdStoreReader as a rangeReader.
type coldAdapter struct{ *ledger.ColdStoreReader }

func (c coldAdapter) iterateRange(start, end uint32, fn func(uint32, []byte) bool) error {
	for entry, err := range c.IterateLedgers(start, end) {
		if err != nil {
			return err
		}
		if !fn(entry.Seq, entry.Bytes) {
			return nil
		}
	}
	return nil
}

// openReader returns a tier-appropriate reader plus the [first, last]
// inclusive seq range it covers.
func openReader(
	logger *supportlog.Entry,
	tier, coldDir, hotDir string,
	chunkID uint32,
	dec *zstd.Decompressor,
) (rangeReader, uint32, uint32, error) {
	first := chunkFirstLedger(chunkID)
	last := chunkLastLedger(chunkID)
	switch tier {
	case "hot":
		h, err := ledger.NewHotStore(hotDir, logger)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("NewHotStore: %w", err)
		}
		if _, err := h.GetLedgerRaw(first); err != nil {
			h.Close()
			return nil, 0, 0, fmt.Errorf("hot store missing seq %d: %w", first, err)
		}
		if _, err := h.GetLedgerRaw(last); err != nil {
			h.Close()
			return nil, 0, 0, fmt.Errorf("hot store missing seq %d: %w", last, err)
		}
		return hotAdapter{h}, first, last, nil

	case "cold":
		path := packPath(coldDir, chunkID)
		c, err := ledger.NewColdStoreReader(path, dec)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("NewColdStoreReader %s: %w", path, err)
		}
		f, ferr := c.FirstSeq()
		l, lerr := c.LastSeq()
		if ferr != nil || lerr != nil {
			c.Close()
			return nil, 0, 0, fmt.Errorf("cold seq probe: %w %w", ferr, lerr)
		}
		if f != first || l != last {
			c.Close()
			return nil, 0, 0, fmt.Errorf("cold range mismatch: got [%d,%d], expected [%d,%d]", f, l, first, last)
		}
		return coldAdapter{c}, first, last, nil

	default:
		return nil, 0, 0, fmt.Errorf("unknown --tier=%q (want hot|cold)", tier)
	}
}
