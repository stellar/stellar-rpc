package methods

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/creachadair/jrpc2"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

// renderedLatestLedger is one ledger's rendered getLatestLedger response.
type renderedLatestLedger struct {
	seq      uint32
	response protocol.GetLatestLedgerResponse
}

// latestLedgerCache memoizes the rendered response keyed by ledger sequence: a
// ledger's bytes never change, and a newly closed ledger invalidates the memo
// by moving the key.
type latestLedgerCache struct {
	ledgerReader store.LedgerReader

	rendered atomic.Pointer[renderedLatestLedger]
	renderMu sync.Mutex // serializes misses so a new ledger renders once, not once per waiting request
}

// NewGetLatestLedgerHandler returns a JSON RPC handler to retrieve the latest ledger entry from Stellar core.
// Requests landing on the same latest ledger share one rendering of it.
func NewGetLatestLedgerHandler(ledgerReader store.LedgerReader) jrpc2.Handler {
	c := &latestLedgerCache{ledgerReader: ledgerReader}
	return NewHandler(c.handle)
}

func (c *latestLedgerCache) handle(ctx context.Context, _ protocol.GetLatestLedgerRequest,
) (protocol.GetLatestLedgerResponse, error) {
	latestSequence, err := c.ledgerReader.GetLatestLedgerSequence(ctx)
	if err != nil {
		return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: "could not get latest ledger sequence",
		}
	}
	if r := c.rendered.Load(); r != nil && r.seq == latestSequence {
		return r.response, nil
	}

	c.renderMu.Lock()
	defer c.renderMu.Unlock()
	if r := c.rendered.Load(); r != nil && r.seq == latestSequence { // rendered while waiting for the lock
		return r.response, nil
	}
	response, err := c.render(ctx, latestSequence)
	if err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	// A request on an older read view must not evict a newer ledger's render.
	if r := c.rendered.Load(); r == nil || latestSequence >= r.seq {
		c.rendered.Store(&renderedLatestLedger{seq: latestSequence, response: response})
	}
	return response, nil
}

func (c *latestLedgerCache) render(ctx context.Context, latestSequence uint32,
) (protocol.GetLatestLedgerResponse, error) {
	var response protocol.GetLatestLedgerResponse
	var parseErr error
	found, err := c.ledgerReader.WithLedgerRaw(ctx, latestSequence, func(raw []byte) error {
		response, parseErr = latestLedgerResponse(xdr.LedgerCloseMetaView(raw), latestSequence)
		return parseErr
	})
	if err != nil || !found {
		var msg string
		switch {
		case parseErr != nil:
			msg = fmt.Sprintf("could not parse latest ledger header: %v", parseErr)
		case err != nil:
			msg = fmt.Sprintf("could not get latest ledger: %v", err)
		default: // clean miss: no underlying error to report
			msg = "could not get latest ledger"
		}
		return protocol.GetLatestLedgerResponse{}, &jrpc2.Error{
			Code:    jrpc2.InternalError,
			Message: msg,
		}
	}
	return response, nil
}

// latestLedgerResponse extracts the response fields from a ledger close meta view.
func latestLedgerResponse(view xdr.LedgerCloseMetaView, sequence uint32,
) (protocol.GetLatestLedgerResponse, error) {
	headerEntry, err := view.LedgerHeader()
	if err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return xdr.Try(func() protocol.GetLatestLedgerResponse {
		header := headerEntry.MustHeader()
		return protocol.GetLatestLedgerResponse{
			Hash:            hex.EncodeToString(headerEntry.MustHash().MustRaw()),
			ProtocolVersion: header.MustLedgerVersion().MustValue(),
			Sequence:        sequence,
			LedgerCloseTime: int64(header.MustScpValue().MustCloseTime().MustValue()), //nolint:gosec // safe for ~292B years
			LedgerHeader:    base64.StdEncoding.EncodeToString(header.MustRaw()),
			LedgerMetadata:  base64.StdEncoding.EncodeToString(view),
		}
	})
}
