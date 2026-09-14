// Package verify checks a cold tree's frozen chunks against the ledgers they
// were built from. Every ledger in a chunk's pack is decoded into Go structs
// and checked as a source: the header names its slot, hashes to the hash
// stored beside it, chains to the previous ledger, and commits to the stored
// envelopes and results. The chunk's events and tx-hash artifacts are then
// compared with what the SDK's decode path derives from those structs, so a
// divergence in the view-based extractors that wrote them, a chunk written by
// an older binary, or a damaged file all show up as mismatches.
package verify

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/stellar/go-stellar-sdk/historyarchive"
	supportlog "github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/support/storage"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// Options configures one run.
type Options struct {
	Layout     geometry.Layout
	Passphrase string
	// StartChunk and EndChunk bound the chunks checked, inclusive; -1 leaves
	// the bound open.
	StartChunk int64
	EndChunk   int64
	// Workers is how many chunks are checked at once; 0 means one per CPU.
	Workers int
	// ArchiveURL, when set, anchors each chunk's first header hash to the
	// network's history archive.
	ArchiveURL string
	// MaxMismatches caps the mismatches recorded per chunk; 0 means 50.
	MaxMismatches int
}

func (o Options) withDefaults() Options {
	if o.Workers <= 0 {
		o.Workers = backfill.DefaultWorkers()
	}
	if o.MaxMismatches <= 0 {
		o.MaxMismatches = 50
	}
	return o
}

// target is one chunk to check with the kinds the catalog holds frozen.
type target struct {
	chunk  chunk.ID
	frozen catalog.ArtifactSet
}

// Run verifies every frozen chunk in range. The catalog is opened read-only,
// so a run can sit beside a live daemon. The returned error is an
// infrastructure failure of the run itself; verdicts are in the Report.
func Run(ctx context.Context, logger *supportlog.Entry, opts Options) (*Report, error) {
	opts = opts.withDefaults()
	if opts.Passphrase == "" {
		return nil, errors.New("verify: network passphrase is required")
	}
	txl, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.OpenReadOnly(opts.Layout.CatalogPath(), opts.Layout, txl, logger)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	defer func() { _ = cat.Close() }()

	targets, err := frozenChunks(cat, opts)
	if err != nil {
		return nil, err
	}
	indexes, err := openIndexes(cat, targets)
	if err != nil {
		return nil, err
	}
	defer func() { _ = indexes.closeAll() }()
	d := &deps{opts: opts, cat: cat, indexes: indexes}
	if opts.ArchiveURL != "" {
		d.archive, err = historyarchive.Connect(opts.ArchiveURL, historyarchive.ArchiveOptions{
			NetworkPassphrase: opts.Passphrase,
			ConnectOptions:    storage.ConnectOptions{Context: ctx, UserAgent: "stellar-rpc-verify-cold"},
		})
		if err != nil {
			return nil, fmt.Errorf("connect history archive: %w", err)
		}
	}

	logger.Infof("verifying %d chunks with %d workers", len(targets), opts.Workers)
	results := runChunks(ctx, logger, d, targets, opts.Workers)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Report{Chunks: results, Indexes: checkIndexes(indexes, results)}, nil
}

func runChunks(ctx context.Context, logger *supportlog.Entry, d *deps, targets []target, workers int) []ChunkResult {
	results := make([]ChunkResult, len(targets))
	var g errgroup.Group
	g.SetLimit(workers)
	for i, t := range targets {
		g.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			results[i] = verifyChunk(ctx, d, t)
			logger.WithField("chunk", t.chunk.String()).Infof("chunk %s: %s (%d mismatches)",
				t.chunk, results[i].status(), len(results[i].Mismatches))
			return nil
		})
	}
	_ = g.Wait() // verdicts live in results; a canceled run is reported by the caller
	return results
}

// verifyChunk checks one chunk whose frozen kinds are given. The ledgers pack
// is the source, so it is checked first; a chunk whose ledgers fail is
// reported for that alone, and its derived artifacts are not compared.
func verifyChunk(ctx context.Context, d *deps, t target) ChunkResult {
	res := ChunkResult{Chunk: t.chunk, Kinds: t.frozen.Kinds()}
	if !t.frozen.Has(geometry.KindLedgers) {
		res.Skipped = "ledgers artifact not frozen"
		return res
	}
	r := &chunkRun{d: d, c: t.chunk, frozen: t.frozen, rec: &recorder{limit: d.opts.MaxMismatches}}
	res.Err = r.run(ctx)
	res.Mismatches, res.Dropped = r.rec.out, r.rec.dropped
	res.Ledgers, res.Txs, res.TxHashes = r.ledgers, r.txs, r.txHashes
	res.Invokes, res.InvokesUnchecked = r.invokes, r.invokesUnchecked
	res.IndexChecked = r.index != nil && res.Err == nil && !r.sourceBad
	if r.events != nil {
		res.Events = uint64(r.events.nextID)
	}
	return res
}

// frozenChunks lists the chunks in range that have any frozen artifact, with
// the frozen kinds of each, ascending.
func frozenChunks(cat *catalog.Catalog, opts Options) ([]target, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return nil, fmt.Errorf("list chunk artifacts: %w", err)
	}
	byChunk := make(map[chunk.ID]catalog.ArtifactSet)
	for _, ref := range refs {
		if ref.State != geometry.StateFrozen {
			continue
		}
		if opts.StartChunk >= 0 && int64(ref.Chunk) < opts.StartChunk {
			continue
		}
		if opts.EndChunk >= 0 && int64(ref.Chunk) > opts.EndChunk {
			continue
		}
		byChunk[ref.Chunk] = byChunk[ref.Chunk].Add(ref.Kind)
	}
	targets := make([]target, 0, len(byChunk))
	for c, kinds := range byChunk {
		targets = append(targets, target{chunk: c, frozen: kinds})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].chunk < targets[j].chunk })
	return targets, nil
}

// checkIndexes compares each resolved tx-hash index with the number of
// hashes the oracle expects across its coverage. A coverage with a chunk
// this run did not index-check is skipped with the reason.
func checkIndexes(ic *indexCache, results []ChunkResult) []IndexResult {
	byChunk := make(map[chunk.ID]ChunkResult, len(results))
	for _, r := range results {
		byChunk[r.Chunk] = r
	}
	entries := ic.entries()
	out := make([]IndexResult, 0, len(entries))
	for _, e := range entries {
		out = append(out, checkIndex(e, byChunk))
	}
	return out
}

func checkIndex(e *indexEntry, byChunk map[chunk.ID]ChunkResult) IndexResult {
	res := IndexResult{Coverage: e.cov}
	for c := e.cov.Lo; c <= e.cov.Hi; c++ {
		r, ok := byChunk[c]
		switch {
		case !ok || r.Skipped != "":
			res.Skipped = fmt.Sprintf("chunk %s not verified in this run", c)
		case r.Err != nil:
			res.Skipped = fmt.Sprintf("chunk %s errored", c)
		case !r.IndexChecked:
			res.Skipped = fmt.Sprintf("chunk %s did not complete its index check", c)
		}
		if res.Skipped != "" {
			return res
		}
		res.Expected += r.TxHashes
	}
	res.Actual = e.reader.KeyCount()
	return res
}
