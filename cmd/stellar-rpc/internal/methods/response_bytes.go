package methods

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/xdr2json"
)

// ByteLimit converts a configured byte limit to an int.
func ByteLimit(n uint) int {
	return int(min(n, math.MaxInt)) //nolint:gosec // min clamps it
}

// PageBudget ends a page before an item that would take its XDR or JSON past
// Limit bytes. A zero Limit means no limit.
type PageBudget struct {
	Limit int
	used  int
	full  bool
}

// Fits counts an item of n bytes if the page has room for it. The first item
// always fits, so paging always moves forward.
func (b *PageBudget) Fits(n int) bool {
	if b.used > 0 && b.Limit > 0 && b.used+n > b.Limit {
		b.full = true
		return false
	}
	b.used += n
	return true
}

// ResponsePool is a request's share of the bridge's response byte pool. Take
// asks it for n more bytes, which a renderer then holds until the response
// is written, or returns the pool's error when there is no room.
type ResponsePool interface{ Take(n int64) error }

type responsePoolKey struct{}

// WithResponsePool returns ctx carrying the request's share of the pool.
func WithResponsePool(ctx context.Context, pool ResponsePool) context.Context {
	return context.WithValue(ctx, responsePoolKey{}, pool)
}

// TakeResponseBytes takes n bytes of the response from the request's pool
// share, if ctx carries one, or returns the pool's error when it has no room.
func TakeResponseBytes(ctx context.Context, n int) error {
	pool, ok := ctx.Value(responsePoolKey{}).(ResponsePool)
	if !ok {
		return nil
	}
	return pool.Take(int64(n))
}

// transactionXDRLen is the base64 size of tx's XDR fields in a response.
func transactionXDRLen(tx store.Transaction) int {
	n := len(tx.Result) + len(tx.Meta) + len(tx.Envelope)
	for _, e := range tx.Events {
		n += len(e)
	}
	for _, e := range tx.TransactionEvents {
		n += len(e)
	}
	for _, op := range tx.ContractEvents {
		for _, e := range op {
			n += len(e)
		}
	}
	return base64.StdEncoding.EncodedLen(n)
}

// xdrBytes is the size of base64 XDR in a response.
func xdrBytes(fields ...string) int {
	n := 0
	for _, f := range fields {
		n += len(f)
	}
	return n
}

// jsonBytes is the size of JSON in a response, after escaping.
func jsonBytes(fields ...json.RawMessage) int {
	n := 0
	for _, f := range fields {
		n += xdr2json.EscapedLen(f)
	}
	return n
}

// ledgerInfoBytes is the size of l's XDR or JSON in a response.
func ledgerInfoBytes(l protocol.LedgerInfo) int {
	return xdrBytes(l.LedgerHeader, l.LedgerMetadata) + jsonBytes(l.LedgerHeaderJSON, l.LedgerMetadataJSON)
}

// transactionInfoBytes is the size of tx's XDR or JSON in a response.
func transactionInfoBytes(tx protocol.TransactionInfo) int {
	n := xdrBytes(tx.EnvelopeXDR, tx.ResultXDR, tx.ResultMetaXDR) +
		xdrBytes(tx.DiagnosticEventsXDR...) + xdrBytes(tx.Events.TransactionEventsXDR...) +
		jsonBytes(tx.EnvelopeJSON, tx.ResultJSON, tx.ResultMetaJSON) +
		jsonBytes(tx.DiagnosticEventsJSON...) + jsonBytes(tx.Events.TransactionEventsJSON...)
	for _, op := range tx.Events.ContractEventsXDR {
		n += xdrBytes(op...)
	}
	for _, op := range tx.Events.ContractEventsJSON {
		n += jsonBytes(op...)
	}
	return n
}

// EventInfoBytes is the size of e's XDR or JSON in a response.
func EventInfoBytes(e protocol.EventInfo) int {
	return xdrBytes(e.ValueXDR) + xdrBytes(e.TopicXDR...) + jsonBytes(e.ValueJSON) + jsonBytes(e.TopicJSON...)
}
