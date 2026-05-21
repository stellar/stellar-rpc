package main

// query_corpus.go is the shared input loader for the cold-events and
// hot-events benches. Operators supply a JSON file describing the
// queries they want to drive against an events.Reader; each request
// translates 1:1 into an eventstore.Query call.
//
// JSON shape (array of requests):
//
//	[
//	  {
//	    "name": "optional human label",
//	    "filters": [
//	      {
//	        "contractId": "<32-byte hex>",
//	        "topic0": "<hex>", "topic1": "<hex>",
//	        "topic2": "<hex>", "topic3": "<hex>"
//	      }
//	    ],
//	    "maxEvents": 100,
//	    "ledgerRange": { "start": 5000, "end": 5050 }
//	  },
//	  { "filters": [] }
//	]
//
// Field semantics:
//
//   - filters [] (or omitted) → match-all
//   - filter object's contractId / topicN omitted or empty → wildcard
//   - maxEvents omitted → CLI default (-max-fetch)
//   - maxEvents: 0 → explicit "unlimited"
//   - ledgerRange omitted → whole ingested chunk
//   - ledgerRange.start / .end == 0 → clipped to chunk's natural bound
//
// Hex encoding throughout: contractIds are the raw 32-byte hashes
// (no strkey wrapping), topic values are the bytes produced by
// xdr.ScVal.MarshalBinary() — i.e., what the storage layer indexed.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores/eventstore"
)

// filterRequest mirrors eventstore.Filter as JSON, with all fields
// optional and hex-encoded.
type filterRequest struct {
	ContractID string `json:"contractId,omitempty"`
	Topic0     string `json:"topic0,omitempty"`
	Topic1     string `json:"topic1,omitempty"`
	Topic2     string `json:"topic2,omitempty"`
	Topic3     string `json:"topic3,omitempty"`
}

// ledgerRangeJSON mirrors eventstore.LedgerRange as JSON.
type ledgerRangeJSON struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
}

// queryRequest is one entry in the JSON corpus.
//
// MaxEvents is a pointer so we can distinguish "omitted" (nil → use
// CLI default) from "explicitly zero" (non-nil 0 → unlimited).
type queryRequest struct {
	Name        string           `json:"name,omitempty"`
	Filters     []filterRequest  `json:"filters"`
	MaxEvents   *int             `json:"maxEvents,omitempty"`
	LedgerRange *ledgerRangeJSON `json:"ledgerRange,omitempty"`
}

// loadQueries reads and validates a JSON query corpus. Unknown
// fields are rejected so typos in the JSON surface as errors rather
// than silently-ignored config.
func loadQueries(path string) ([]queryRequest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open queries file %s: %w", path, err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var out []queryRequest
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("parse queries file %s: %w", path, err)
	}
	if len(out) == 0 {
		return nil, errors.New("queries file is empty")
	}
	return out, nil
}

// toFilters converts the request's filters into eventstore.Filter
// slice. Returns nil filters slice for a match-all request (empty
// filters array in JSON).
//
// Validates field lengths so a typo'd hex string doesn't silently
// produce a filter that hashes wrong-shaped bytes and matches
// nothing — a wrong-length filter would corrupt bench results
// without any error surface. ContractID is the raw 32-byte hash;
// topic values are XDR-marshaled ScVals which are always ≥4 bytes
// (the XDR discriminator), so any decoded length of 0 is a typo.
func (qr queryRequest) toFilters() ([]eventstore.Filter, error) {
	if len(qr.Filters) == 0 {
		return nil, nil
	}
	const contractIDLen = 32
	out := make([]eventstore.Filter, len(qr.Filters))
	for i, f := range qr.Filters {
		var ef eventstore.Filter
		if f.ContractID != "" {
			b, err := hex.DecodeString(f.ContractID)
			if err != nil {
				return nil, fmt.Errorf("filter %d contractId: %w", i, err)
			}
			if len(b) != contractIDLen {
				return nil, fmt.Errorf("filter %d contractId: expected %d bytes, got %d",
					i, contractIDLen, len(b))
			}
			ef.ContractID = b
		}
		topics := [protocol.MaxTopicCount]string{f.Topic0, f.Topic1, f.Topic2, f.Topic3}
		for ti, raw := range topics {
			if raw == "" {
				continue
			}
			b, err := hex.DecodeString(raw)
			if err != nil {
				return nil, fmt.Errorf("filter %d topic%d: %w", i, ti, err)
			}
			if len(b) == 0 {
				return nil, fmt.Errorf("filter %d topic%d: empty after hex decode", i, ti)
			}
			ef.Topics[ti] = b
		}
		out[i] = ef
	}
	return out, nil
}

// toOptions builds the eventstore.QueryOptions. defaultMaxEvents is
// the CLI -max-fetch value, used when the JSON request omits
// maxEvents. A JSON-explicit maxEvents: 0 disables the cap entirely
// for that request (per the QueryOptions API: 0 = unlimited).
func (qr queryRequest) toOptions(defaultMaxEvents int) eventstore.QueryOptions {
	opts := eventstore.QueryOptions{MaxEvents: defaultMaxEvents}
	if qr.MaxEvents != nil {
		opts.MaxEvents = *qr.MaxEvents
	}
	if qr.LedgerRange != nil {
		opts.LedgerRange = eventstore.LedgerRange{
			Start: qr.LedgerRange.Start,
			End:   qr.LedgerRange.End,
		}
	}
	return opts
}

// preparedQuery is a queryRequest pre-translated to the
// eventstore-native shapes so per-iter cost stays out of the hot
// path.
type preparedQuery struct {
	idx          int    // 0-based position in the JSON array; used as CSV demux key
	name         string // optional human label
	filters      []eventstore.Filter
	opts         eventstore.QueryOptions
	nUniqueTerms int // count of non-empty (contractId + topic[0..3]) slots across filters
}

// label returns the demux-key form used in CSV error context and
// stats labels — "[idx]" without a name, "[idx:name]" with one.
func (p preparedQuery) label() string {
	if p.name == "" {
		return fmt.Sprintf("[%d]", p.idx)
	}
	return fmt.Sprintf("[%d:%s]", p.idx, p.name)
}

// prepareQueries pre-translates the JSON corpus, applying
// defaultMaxEvents to any request that omits maxEvents. Counts
// non-empty filter slots so the bench's n_unique_terms column
// reflects what eventstore.Query will look up (the count is a
// conservative upper bound — same value used twice gets counted
// twice; Query's runtime dedupe would collapse it).
func prepareQueries(reqs []queryRequest, defaultMaxEvents int) ([]preparedQuery, error) {
	out := make([]preparedQuery, len(reqs))
	for i, qr := range reqs {
		filters, err := qr.toFilters()
		if err != nil {
			return nil, fmt.Errorf("query[%d] %s: %w", i, qr.Name, err)
		}
		slots := 0
		for _, f := range filters {
			if len(f.ContractID) > 0 {
				slots++
			}
			for _, t := range f.Topics {
				if len(t) > 0 {
					slots++
				}
			}
		}
		out[i] = preparedQuery{
			idx:          i,
			name:         qr.Name,
			filters:      filters,
			opts:         qr.toOptions(defaultMaxEvents),
			nUniqueTerms: slots,
		}
	}
	return out, nil
}
