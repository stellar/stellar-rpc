package network

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/creachadair/jrpc2"
)

// MakeJSONRPCRequestParser returns a jhttp.BridgeOptions.ParseRequest hook that
// rejects batches of more than maxBatchSize calls, and notifications (calls
// without an id), before the bridge dispatches anything.
func MakeJSONRPCRequestParser(maxBatchSize uint) func(*http.Request) ([]*jrpc2.ParsedRequest, error) {
	return func(r *http.Request) ([]*jrpc2.ParsedRequest, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
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

// RequireJSONPost makes the checks jhttp.Bridge skips once a ParseRequest hook
// is set: the method must be POST and the content type application/json.
func RequireJSONPost(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Post", "application/json")
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		mt, params, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if mt != "application/json" {
			http.Error(w, "content-type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		if cs, ok := params["charset"]; ok && !strings.EqualFold(cs, "utf-8") && !strings.EqualFold(cs, "utf8") {
			http.Error(w, "invalid content-type charset", http.StatusUnsupportedMediaType)
			return
		}
		next.ServeHTTP(w, r)
	})
}
