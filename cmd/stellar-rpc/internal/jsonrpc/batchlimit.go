package jsonrpc

import (
	"bytes"
	"encoding/json"

	"github.com/creachadair/jrpc2"
)

// parseRequests parses body as jrpc2.ParseRequests does, but turns a batch of
// more than maxBatchSize calls, an empty batch, and each notification (a call
// without an id) into errors, which the bridge answers without dispatching.
func parseRequests(body []byte, maxBatchSize uint) ([]*jrpc2.ParsedRequest, error) {
	if batchExceeds(body, maxBatchSize) {
		// A batch gets a batch reply, here holding one error.
		return []*jrpc2.ParsedRequest{{
			Batch: true,
			Error: jrpc2.Errorf(jrpc2.InvalidRequest, "batch exceeds the limit of %d calls", maxBatchSize),
		}}, nil
	}
	reqs, err := jrpc2.ParseRequests(body)
	if err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		return []*jrpc2.ParsedRequest{{Error: jrpc2.Errorf(jrpc2.InvalidRequest, "empty batch")}}, nil
	}
	for _, req := range reqs {
		if req.Error == nil && req.ID == "" {
			req.Error = jrpc2.Errorf(jrpc2.InvalidRequest, "notifications are not supported")
		}
	}
	return reqs, nil
}

// batchExceeds reports whether body is a JSON array of more than limit
// elements. It stops reading at the first element past the limit, so an
// oversized batch costs no more than one at the limit.
func batchExceeds(body []byte, limit uint) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('[') {
		return false
	}
	var elem json.RawMessage
	for n := uint(0); dec.More(); n++ {
		if n == limit {
			return true
		}
		if dec.Decode(&elem) != nil {
			return false
		}
	}
	return false
}
