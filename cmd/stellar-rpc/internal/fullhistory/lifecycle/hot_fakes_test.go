package lifecycle

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stretchr/testify/require"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/backfill"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/chunk"
)

// fakeHotChunk is a test backfill.HotChunk: a hand-set MaxCommittedSeq + an
// injectable LedgerStream source, counting closes when closedTo is non-nil.
type fakeHotChunk struct {
	maxSeq   uint32
	present  bool
	maxErr   error
	source   ledgerbackend.LedgerStream
	closedTo *atomic.Int32
}

func (h *fakeHotChunk) MaxCommittedSeq() (uint32, bool, error) {
	return h.maxSeq, h.present, h.maxErr
}
func (h *fakeHotChunk) Source() ledgerbackend.LedgerStream { return h.source }
func (h *fakeHotChunk) Close() error {
	if h.closedTo != nil {
		h.closedTo.Add(1)
	}
	return nil
}

// fakeHotProbe is a test backfill.HotProbe: returns its fake chunk when ok, an
// error when openErr is set, or (nil,false,nil) for "no ready hot DB". Counts
// opens via openedTo when non-nil.
type fakeHotProbe struct {
	chunk    *fakeHotChunk
	ok       bool
	openErr  error
	openedTo *atomic.Int32
}

func (p *fakeHotProbe) OpenHotChunk(chunk.ID) (backfill.HotChunk, bool, error) {
	if p.openedTo != nil {
		p.openedTo.Add(1)
	}
	if p.openErr != nil {
		return nil, false, p.openErr
	}
	if !p.ok {
		return nil, false, nil
	}
	return p.chunk, true, nil
}

// writeArtifact writes a placeholder artifact file at path (creating parents),
// so a test can assert presence/absence around the catalog protocol.
func writeArtifact(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("artifact"), 0o644))
}

// hotKeyExists reports whether chunk c has a hot:chunk key (any value). The
// catalog's key existence read is unexported; this is the streaming-package test
// shim over the public HotState ("" ⇒ absent).
func hotKeyExists(cat *catalog.Catalog, c chunk.ID) (bool, error) {
	s, err := cat.HotState(c)
	return s != "", err
}

func TestRoundTripHotKeys(t *testing.T) {
	cat, _ := testCatalog(t)

	state, err := cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)

	require.NoError(t, cat.PutHotTransient(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotTransient, state)

	require.NoError(t, cat.FlipHotReady(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotReady, state)

	require.NoError(t, cat.DeleteHotKey(7))
	state, err = cat.HotState(7)
	require.NoError(t, err)
	require.Equal(t, geometry.HotState(""), state)
	// Idempotent on a missing key.
	require.NoError(t, cat.DeleteHotKey(7))
}
