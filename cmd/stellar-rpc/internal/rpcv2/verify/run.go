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
	"math"
	"slices"
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
	// ArchiveURL, when set, anchors each chunk's last header hash to the
	// network's history archive; the chain then authenticates every header
	// before it.
	ArchiveURL string
	// MaxMismatches caps the mismatches recorded per chunk; 0 means 50.
	MaxMismatches int

	// beforeOpen, when set, runs before a chunk's files are opened, and
	// anchor, when set, replaces the history archive. Test seams for racing
	// the run against catalog changes and for anchoring without a network.
	beforeOpen func(chunk.ID)
	anchor     headerAnchor
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

func (o Options) validate() error {
	if o.Passphrase == "" {
		return errors.New("verify: network passphrase is required")
	}
	if o.StartChunk < -1 || o.EndChunk < -1 {
		return fmt.Errorf("verify: chunk bounds must be -1 or a chunk id, got start %d end %d", o.StartChunk, o.EndChunk)
	}
	if o.StartChunk >= 0 && o.EndChunk >= 0 && o.StartChunk > o.EndChunk {
		return fmt.Errorf("verify: start chunk %d is past end chunk %d", o.StartChunk, o.EndChunk)
	}
	// A chunk id is a uint32. Without this an end bound above the maximum
	// truncates into range silently, and the maximum itself makes a c <= hi
	// loop wrap around forever.
	if o.StartChunk > math.MaxUint32 || o.EndChunk > math.MaxUint32 {
		return fmt.Errorf("verify: chunk bounds must be at most %d, got start %d end %d",
			uint32(math.MaxUint32), o.StartChunk, o.EndChunk)
	}
	if o.MaxMismatches < 0 {
		return fmt.Errorf("verify: max mismatches must be 0 for the default or a positive count, got %d",
			o.MaxMismatches)
	}
	return nil
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
	// Before withDefaults, which maps a non-positive cap to 50 and would hide
	// a negative one from validate.
	if err := opts.validate(); err != nil {
		return nil, err
	}
	opts = opts.withDefaults()
	txl, err := geometry.NewTxHashIndexLayout(geometry.ChunksPerTxhashIndex)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.OpenReadOnly(opts.Layout.CatalogPath(), opts.Layout, txl, logger)
	if err != nil {
		return nil, fmt.Errorf("open catalog: %w", err)
	}
	defer func() { _ = cat.Close() }()

	targets, absent, absentTotal, err := frozenChunks(cat, opts)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return &Report{Absent: absent, AbsentCount: absentTotal}, errors.New("verify: no frozen chunks in range")
	}
	indexes := newIndexCache(cat.Layout())
	defer func() { _ = indexes.closeAll() }()
	d := &deps{opts: opts, cat: cat, indexes: indexes, archive: opts.anchor}
	if opts.ArchiveURL != "" {
		archive, err := historyarchive.Connect(opts.ArchiveURL, historyarchive.ArchiveOptions{
			NetworkPassphrase: opts.Passphrase,
			ConnectOptions:    storage.ConnectOptions{Context: ctx, UserAgent: "stellar-rpc-verify-cold"},
		})
		if err != nil {
			return nil, fmt.Errorf("connect history archive: %w", err)
		}
		d.archive = archive
	}

	logger.Infof("verifying %d chunks with %d workers", len(targets), opts.Workers)
	results := runChunks(ctx, logger, d, targets, opts.Workers)
	report := &Report{
		Chunks: results, Indexes: checkIndexes(indexes, results),
		Absent: absent, AbsentCount: absentTotal,
	}
	// A canceled run still has verdicts for the chunks it finished, so the
	// report comes back either way and the error says the run is incomplete.
	// The caller prints the summary and then surfaces this error, so an
	// interrupted run reports what it learned AND still exits non-zero.
	if err := ctx.Err(); err != nil {
		return report, err
	}
	if !slices.ContainsFunc(results, func(r ChunkResult) bool { return r.Status != statusSkipped }) {
		return report, errors.New("verify: no chunk in range has a frozen ledgers pack to verify against")
	}
	return report, nil
}

func runChunks(ctx context.Context, logger *supportlog.Entry, d *deps, targets []target, workers int) []ChunkResult {
	results := make([]ChunkResult, len(targets))
	// Seed every slot before any worker starts. A slot a canceled run never
	// reaches keeps this value, so it carries its real chunk id (which
	// checkIndexes needs) and reads as not-run rather than as clean.
	for i, t := range targets {
		results[i] = ChunkResult{Chunk: t.chunk, Kinds: t.frozen.Kinds(), Status: statusNotRun}
	}
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
// is the source, so it is checked first, as a whole (its content hash and
// its archive anchor) and then ledger by ledger. Derived artifacts are
// compared for every ledger up to the first that fails a source check; from
// there on only the source checks continue, so nothing is compared against
// a source the run no longer trusts.
func verifyChunk(ctx context.Context, d *deps, t target) ChunkResult {
	res := ChunkResult{Chunk: t.chunk, Kinds: t.frozen.Kinds()}
	if !t.frozen.Has(geometry.KindLedgers) {
		res.Checks.unexplained(
			"the catalog names no frozen ledgers pack for this chunk, so there was nothing to check anything against")
		res.Status = statusSkipped
		return res
	}
	r := &chunkRun{d: d, c: t.chunk, frozen: t.frozen, rec: &recorder{limit: d.opts.MaxMismatches}}
	res.Err = r.run(ctx)
	res.Err = errors.Join(res.Err, r.releaseIndex())
	res.Mismatches, res.Dropped = r.rec.out, r.rec.dropped
	res.Ledgers, res.Txs, res.TxHashes = r.ledgers, r.txs, r.txHashes
	res.Invokes, res.InvokesUnchecked = r.invokes, r.invokesUnchecked
	if r.events != nil {
		res.Events = r.events.checked
	}
	// Keep the findings: every row describes bytes really read, and run()
	// already skips the chunk-wide totals a partial pass would distort.
	// Failed() keys on the findings, Incomplete() on the status.
	canceled := ctx.Err() != nil
	res.Checks = r.outcomes(canceled)
	if canceled {
		res.Status = statusCanceled
		return res
	}
	// Gated exactly as checkLedgers is, so the two can never disagree about one
	// chunk; see the field's doc for why it is not a check.
	res.ResolvedThroughIndex = r.index != nil && r.walked && !r.sourceBad
	res.Status = classify(res.Err, len(res.Mismatches))
	return res
}

// frozenChunks lists the chunks in range that have any frozen artifact, with
// the frozen kinds of each, ascending, and separately the chunks in the range
// the catalog does not name at all. A chunk with nothing frozen never becomes
// a target, so without that second return it is absent from the report, the
// summary's denominator and the exit status.
func frozenChunks(cat *catalog.Catalog, opts Options) ([]target, []chunk.ID, int, error) {
	refs, err := cat.ChunkArtifactKeys()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("list chunk artifacts: %w", err)
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
	absent, total := absentChunks(byChunk, targets, opts)
	return targets, absent, total, nil
}

// absentChunks lists the chunks the run was asked for that the catalog names
// no frozen artifact for. The span is the requested range where the operator
// gave bounds, and the frozen extent where they did not — so a default run
// reports holes in the middle of its own history, and a bounded run also
// reports a range that runs off the end of what has been backfilled. With
// nothing frozen there is no extent, so only a range bounded on both sides
// has anything to count.
func absentChunks(byChunk map[chunk.ID]catalog.ArtifactSet, targets []target, opts Options) ([]chunk.ID, int) {
	var lo, hi chunk.ID
	switch {
	case len(targets) > 0:
		lo, hi = targets[0].chunk, targets[len(targets)-1].chunk
	case opts.StartChunk < 0 || opts.EndChunk < 0:
		return nil, 0
	}
	if opts.StartChunk >= 0 {
		lo = chunk.ID(opts.StartChunk) //nolint:gosec // validated non-negative above
	}
	if opts.EndChunk >= 0 {
		hi = chunk.ID(opts.EndChunk) //nolint:gosec // validated non-negative above
	}
	// Counted in full, listed only up to idsListed: a bound far past the
	// end of the data is a legitimate thing to ask for, and answering it with
	// one id per chunk would be tens of megabytes of report.
	var out []chunk.ID
	var total int
	for c := uint64(lo); c <= uint64(hi); c++ {
		if _, ok := byChunk[chunk.ID(c)]; ok {
			continue
		}
		total++
		if len(out) < idsListed {
			out = append(out, chunk.ID(c))
		}
	}
	return out, total
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
		case !ok || r.Status == statusSkipped || r.Status == statusNotRun || r.Status == statusCanceled:
			res.Skipped = fmt.Sprintf("chunk %s not verified in this run", c)
		case r.Err != nil:
			res.Skipped = fmt.Sprintf("chunk %s errored", c)
		case !r.ResolvedThroughIndex:
			res.Skipped = fmt.Sprintf("chunk %s did not complete its index check", c)
		}
		if res.Skipped != "" {
			return res
		}
		res.Expected += r.TxHashes
	}
	res.Actual = e.keyCount
	return res
}
