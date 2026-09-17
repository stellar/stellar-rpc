package verify

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/geometry"
)

// Mismatch is one disagreement between an artifact and what the chunk's
// ledgers say it should hold. Ledger is 0 and TxHash empty for a chunk-wide
// finding.
type Mismatch struct {
	Ledger   uint32
	TxHash   string
	Artifact string
	Field    string
	Expected string
	Actual   string
}

// check names one of the comparisons a chunk's verdict is made of.
//
// "ok" only says that nothing which ran disagreed, so a chunk carries the
// whole set and each comparison that did not happen carries its reason. A
// comparison that never ran has to be as answerable as one that disagreed.
type check int

const (
	checkLedgers  check = iota // the pack's ledgers, header by header
	checkChain                 // the first header against the previous chunk's last
	checkArchive               // the last header against the network's history archive
	checkEvents                // the events segment: payloads, ranges, counts, terms
	checkTxHashes              // the chunk's transaction hashes, through its .bin or its index
)

// numChecks sizes the set. Not a check itself, so a switch over check stays
// exhaustive.
const numChecks = int(checkTxHashes) + 1

// allChecks yields every comparison, in the order a report lists them.
func allChecks(yield func(check) bool) {
	for i := range numChecks {
		if !yield(check(i)) {
			return
		}
	}
}

// against is what a chunk is compared against by this check, for the line that
// says it was not.
func (c check) against() string {
	switch c {
	case checkLedgers:
		return "its own ledger pack, header by header"
	case checkChain:
		return "the previous chunk's last header"
	case checkArchive:
		return "the network's history archive"
	case checkEvents:
		return "its events segment"
	case checkTxHashes:
		return "its transaction hashes"
	}
	return "an unnamed comparison"
}

// counted is what to call a count of these in the summary.
func (c check) counted() string {
	switch c {
	case checkLedgers:
		return "ledger packs"
	case checkChain:
		return "predecessors"
	case checkArchive:
		return "archive headers"
	case checkEvents:
		return "events segments"
	case checkTxHashes:
		return "tx-hash sets"
	}
	return "unnamed comparisons"
}

// outcome is what became of one comparison. The zero value is the safe one:
// it did not run, and nobody said why.
type outcome struct {
	Ran bool
	Why string
}

// checkSet is every comparison's outcome for one chunk. An array rather than
// a map so the zero value is "nothing was compared", which is exactly what a
// result nobody filled in must look like.
type checkSet [numChecks]outcome

// compared records that a comparison ran to completion — unless a reason was
// already recorded, in which case that site was closer to the cause and its
// reason stands.
func (s *checkSet) compared(c check) {
	if s[c].Why == "" {
		s[c].Ran = true
	}
}

// notCompared records why a comparison did not happen. The first reason wins;
// it is the one closest to the cause.
func (s *checkSet) notCompared(c check, why string) {
	if s[c].Why == "" {
		s[c] = outcome{Why: why}
	}
}

// unexplained gives every comparison that neither ran nor recorded a reason
// the one the caller knows, so no gap in a report is ever silent.
func (s *checkSet) unexplained(why string) {
	for c := range allChecks {
		if !s[c].Ran {
			s.notCompared(c, why)
		}
	}
}

// ChunkResult is one chunk's outcome. Err is an infrastructure failure that
// stopped the chunk (an unreadable file, a failed archive fetch), distinct
// from a Mismatch, which is a verdict about the data.
type ChunkResult struct {
	Chunk    chunk.ID
	Kinds    []geometry.Kind
	Ledgers  uint32
	Txs      uint64
	TxHashes uint64
	Events   uint64
	// Invokes is the number of successful Soroban invocations whose events
	// were checked against the hash their result carries; InvokesUnchecked
	// those the export gave no way to check.
	Invokes          uint64
	InvokesUnchecked uint64
	// ResolvedThroughIndex is set when every expected tx hash was resolved
	// through the frozen tx-hash index covering the chunk. Deliberately not
	// one of Checks: that set says the hashes were compared, by whatever
	// means, while a coverage's key count needs to know they went through
	// this index in particular.
	ResolvedThroughIndex bool
	// Checks is what this chunk's contents were compared against, and why
	// each comparison that did not happen did not.
	Checks     checkSet
	Mismatches []Mismatch
	// Dropped is how many mismatches were found and not recorded because the
	// chunk had already reached the cap. A comparison the cap stopped the run
	// making at all is not a finding and is not counted here — it is the
	// reason on the check it belongs to.
	Dropped int
	Err     error
	// Status is the chunk's outcome, set once by the verifier. The ZERO
	// value is statusNotRun on purpose: a ChunkResult that was never filled
	// in — a slot for a chunk a canceled run never reached — must never read
	// as clean. Derive it with classify, never by inspecting the fields.
	Status status
}

// status is a named type so the exhaustive linter checks every switch over
// it; a new outcome must not slip past a default arm.
type status string

const (
	// statusNotRun is the zero value: this chunk was never verified.
	statusNotRun   status = ""
	statusOK       status = "ok"
	statusMismatch status = "mismatch"
	statusError    status = "error"
	statusSkipped  status = "skipped"
	statusCanceled status = "canceled"
)

// classify derives the outcome of a chunk the verifier actually ran.
func classify(err error, mismatches int) status {
	switch {
	case err != nil:
		return statusError
	case mismatches > 0:
		return statusMismatch
	}
	return statusOK
}

// status is the outcome for display. An unset Status is a result that was
// never filled in, which reads as "not run" rather than as success.
func (r ChunkResult) status() string {
	if r.Status == statusNotRun {
		return "not run"
	}
	return string(r.Status)
}

// IndexResult is one frozen tx-hash index coverage's key-count check.
type IndexResult struct {
	Coverage geometry.TxHashIndexCoverage
	Expected uint64
	Actual   uint64
	Skipped  string
}

func (r IndexResult) failed() bool { return r.Skipped == "" && r.Expected != r.Actual }

// Report is one run's outcome.
type Report struct {
	Chunks  []ChunkResult
	Indexes []IndexResult
	// Absent are chunks the run was asked for that the catalog names no
	// frozen artifact for. They have no ChunkResult — there was nothing to
	// verify — so they are counted here, or a run over a range wider than the
	// data would report only what it happened to find. The slice is bounded;
	// AbsentCount is exact.
	Absent      []chunk.ID
	AbsentCount int
}

// Failed reports whether the DATA is wrong: a chunk whose bytes disagree with
// its ledgers, or an index whose key count does not add up. Incompleteness —
// a canceled or never-run chunk — is not a verdict on the data and is
// reported through Incomplete instead, so an interrupted run never claims to
// have found corruption it did not look for.
func (r *Report) Failed() bool {
	for _, c := range r.Chunks {
		// Keyed on recorded mismatches, not on status: a chunk can be
		// incomplete AND have found real corruption before it stopped, and an
		// environment failure (statusError) is not a verdict on the data at
		// all. Both cases are reported through Incomplete instead.
		if len(c.Mismatches) > 0 {
			return true
		}
	}
	for _, ix := range r.Indexes {
		if ix.failed() {
			return true
		}
	}
	return false
}

// Incomplete reports how many chunks this run did not finish checking: never
// started, abandoned mid-flight, stopped by an environment failure such as an
// unreadable file or an unreachable archive, or asked for and not present at
// all. None of these say anything about the data, but each means some of it
// went unexamined, so they drive the command's non-zero exit separately from
// Failed.
func (r *Report) Incomplete() int {
	n := r.AbsentCount
	for _, c := range r.Chunks {
		// statusSkipped counts too: a chunk whose ledgers pack is not frozen
		// has no source to check anything against, so it was asked for and
		// not examined, exactly like one the catalog does not name at all.
		switch c.Status {
		case statusNotRun, statusCanceled, statusError, statusSkipped:
			n++
		case statusOK, statusMismatch:
			// A chunk that reached a verdict. Whether the verdict is good is
			// Failed's question, not this one.
		}
	}
	return n
}

// Summary is the one-line outcome for the log. It reports what the run
// COMPARED as well as what it found, because those are different questions
// and only the first one makes a clean verdict mean anything.
func (r *Report) Summary() string {
	var ok, mismatched, errored, skipped, canceled, notRun int
	var compared [numChecks]int
	var ledgers, txs, events, invokes, unchecked uint64
	for _, c := range r.Chunks {
		for i := range allChecks {
			if c.Checks[i].Ran {
				compared[i]++
			}
		}
		switch c.Status {
		case statusOK:
			ok++
		case statusMismatch:
			mismatched++
		case statusError:
			errored++
		case statusSkipped:
			skipped++
		case statusCanceled:
			canceled++
		case statusNotRun:
			notRun++
		}
		ledgers += uint64(c.Ledgers)
		txs += c.Txs
		events += c.Events
		invokes += c.Invokes
		unchecked += c.InvokesUnchecked
	}
	// Report checked and unchecked coverages, not just failures: a run where
	// every coverage was skipped must not read the same as one where they all
	// passed. A bounded run skips all of them by construction, since a
	// coverage spans a thousand chunks.
	var idxBad, idxChecked, idxSkipped int
	for _, ix := range r.Indexes {
		switch {
		case ix.Skipped != "":
			idxSkipped++
		case ix.failed():
			idxBad++
		default:
			idxChecked++
		}
	}
	against := make([]string, 0, numChecks)
	for i := range allChecks {
		against = append(against, fmt.Sprintf("%d %s", compared[i], i.counted()))
	}
	return fmt.Sprintf(
		"chunks: %d ok, %d with mismatches, %d errored, %d skipped, %d canceled, %d not run, "+
			"%d asked for and not in the catalog; "+
			"%d ledgers, %d transactions, %d events, %d invocation hashes checked (%d not checkable); "+
			"of %d chunks asked for, compared against: %s; "+
			"tx-hash index: %d checked, %d failed, %d not checked",
		ok, mismatched, errored, skipped, canceled, notRun, r.AbsentCount,
		ledgers, txs, events, invokes-unchecked, unchecked,
		len(r.Chunks)+r.AbsentCount, strings.Join(against, ", "),
		idxChecked, idxBad, idxSkipped)
}

// Gap is one reason a set of chunks went uncompared against something.
// Count is exact; Chunks is a sample of them.
type Gap struct {
	Check  check
	Why    string
	Chunks []chunk.ID
	Count  int
}

// gapsSampled bounds the chunk ids carried in one Gap.
const gapsSampled = 5

// gaps groups every comparison that did not run, by what was missed and why.
//
// Grouped rather than filtered, so nothing is hidden. The reason separates a
// property of the run from a property of a chunk: "no history archive was
// given" is one line for every chunk, "chunk 99 is not frozen" names one.
func (r *Report) gaps() []Gap {
	type key struct {
		c   check
		why string
	}
	seen := make(map[key]*Gap)
	order := make([]key, 0, numChecks)
	for _, c := range r.Chunks {
		if c.Status != statusOK && c.Status != statusMismatch {
			// A chunk that errored, was canceled, skipped or never started
			// reports its own reason; listing what it did not compare would
			// read as a coverage gap in data nobody looked at.
			continue
		}
		for i := range allChecks {
			if c.Checks[i].Ran {
				continue
			}
			k := key{i, c.Checks[i].reason()}
			g, ok := seen[k]
			if !ok {
				g = &Gap{Check: i, Why: k.why}
				seen[k] = g
				order = append(order, k)
			}
			g.Count++
			if len(g.Chunks) < gapsSampled {
				g.Chunks = append(g.Chunks, c.Chunk)
			}
		}
	}
	out := make([]Gap, 0, len(order))
	for _, k := range order {
		out = append(out, *seen[k])
	}
	slices.SortFunc(out, func(a, b Gap) int {
		if a.Check != b.Check {
			return int(a.Check) - int(b.Check)
		}
		return strings.Compare(a.Why, b.Why)
	})
	return out
}

// reason is why a comparison did not run, never empty.
func (o outcome) reason() string {
	if o.Why == "" {
		return "no reason recorded"
	}
	return o.Why
}

// recorder collects one chunk's mismatches up to a cap; the overflow is
// counted so the report says how much was left out.
type recorder struct {
	limit   int
	out     []Mismatch
	dropped int
}

func (r *recorder) add(m Mismatch) {
	if len(r.out) >= r.limit {
		r.dropped++
		return
	}
	r.out = append(r.out, m)
}

func (r *recorder) full() bool { return len(r.out) >= r.limit }

func u32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func u64(v uint64) string { return strconv.FormatUint(v, 10) }

func hexHash(h xdr.Hash) string { return h.HexString() }
