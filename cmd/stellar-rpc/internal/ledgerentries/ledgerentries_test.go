package ledgerentries

import (
	"context"
	"encoding/base64"
	"errors"
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	coreProto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const pinnedLedger = 27

// errLedgerNotFound is how the core client reports the 404 core's query
// server answers for a ledger it does not have.
var errLedgerNotFound = errors.New("http request failed with non-200 status code (404)")

// fakeCore answers each read with answer, given the ledger the read is pinned
// to, 0 when it is not pinned, and records the reads.
type fakeCore struct {
	answer func(ledgerSeq uint32) (coreProto.GetLedgerEntryResponse, error)
	reads  []uint32
}

func (f *fakeCore) GetLedgerEntries(
	_ context.Context, ledgerSeq uint32, _ ...xdr.LedgerKeyView,
) (coreProto.GetLedgerEntryResponse, error) {
	f.reads = append(f.reads, ledgerSeq)
	return f.answer(ledgerSeq)
}

// coreBehindPinnedLedger is a core whose query server refuses the first
// refusals reads pinned to pinnedLedger and serves the rest, and answers
// unpinned reads at the ledgers in order, then at the last of them.
func coreBehindPinnedLedger(
	t *testing.T, refusals int, ledgers ...uint32,
) (*fakeCore, xdr.LedgerKeyView, LedgerKeyAndEntry) {
	t.Helper()
	entry := xdr.LedgerEntry{
		LastModifiedLedgerSeq: 1,
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeAccount,
			Account: &xdr.AccountEntry{
				AccountId: xdr.MustAddress("GBRPYHIL2CI3FNQ4BXLFMNDLFJUNPU2HY3ZMFSHONUCEOASW7QC7OX2H"),
				Balance:   10,
			},
		},
	}
	key, err := entry.LedgerKey()
	require.NoError(t, err)
	keyXDR, err := key.MarshalBinary()
	require.NoError(t, err)
	entryXDR, err := entry.MarshalBinary()
	require.NoError(t, err)
	core := &fakeCore{answer: func(ledgerSeq uint32) (coreProto.GetLedgerEntryResponse, error) {
		if ledgerSeq == pinnedLedger && refusals > 0 {
			refusals--
			return coreProto.GetLedgerEntryResponse{}, errLedgerNotFound
		}
		ledger := ledgerSeq
		if ledgerSeq == 0 {
			ledger = ledgers[0]
			if len(ledgers) > 1 {
				ledgers = ledgers[1:]
			}
		}
		return coreProto.GetLedgerEntryResponse{
			Ledger: ledger,
			Entries: []coreProto.LedgerEntryResponse{{
				State: coreProto.LedgerEntryStateLive,
				Entry: base64.StdEncoding.EncodeToString(entryXDR),
			}},
		}, nil
	}}
	return core, keyXDR, LedgerKeyAndEntry{Key: keyXDR, Entry: entryXDR}
}

func TestGetLedgerEntriesWaitsForCoreToReachThePinnedLedger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, key, want := coreBehindPinnedLedger(t, 1, pinnedLedger-1, pinnedLedger-1, pinnedLedger)

		entries, ledger, err := NewLedgerEntryAtGetter(core, pinnedLedger).
			GetLedgerEntries(t.Context(), []xdr.LedgerKeyView{key})
		require.NoError(t, err)
		require.EqualValues(t, pinnedLedger, ledger)
		require.Equal(t, []LedgerKeyAndEntry{want}, entries)
		require.Equal(t, []uint32{pinnedLedger, 0, 0, 0}, core.reads)
	})
}

// Core keeps its last few ledgers, so it can serve the pinned ledger after it
// has moved past it.
func TestGetLedgerEntriesReadsThePinnedLedgerAgainWhenCoreIsPastIt(t *testing.T) {
	core, key, want := coreBehindPinnedLedger(t, 1, pinnedLedger+1)

	entries, ledger, err := NewLedgerEntryAtGetter(core, pinnedLedger).
		GetLedgerEntries(t.Context(), []xdr.LedgerKeyView{key})
	require.NoError(t, err)
	require.EqualValues(t, pinnedLedger, ledger)
	require.Equal(t, []LedgerKeyAndEntry{want}, entries)
	require.Equal(t, []uint32{pinnedLedger, 0, pinnedLedger}, core.reads)
}

func TestGetLedgerEntriesFailsWhenCoreHasDroppedThePinnedLedger(t *testing.T) {
	core, key, _ := coreBehindPinnedLedger(t, 2, pinnedLedger+1)

	_, _, err := NewLedgerEntryAtGetter(core, pinnedLedger).GetLedgerEntries(t.Context(), []xdr.LedgerKeyView{key})
	require.ErrorIs(t, err, errLedgerNotFound)
	require.Equal(t, []uint32{pinnedLedger, 0, pinnedLedger}, core.reads)
}

func TestGetLedgerEntriesDoesNotReadAgainWhenCoreDidNotAnswer(t *testing.T) {
	refused := pkgerrors.Wrap(&url.Error{
		Op: "Post", URL: "http://localhost:11628/getledgerentry",
		Err: errors.New("connection refused"),
	}, "http request errored")
	core := &fakeCore{answer: func(uint32) (coreProto.GetLedgerEntryResponse, error) {
		return coreProto.GetLedgerEntryResponse{}, refused
	}}

	_, _, err := NewLedgerEntryAtGetter(core, pinnedLedger).
		GetLedgerEntries(t.Context(), []xdr.LedgerKeyView{xdr.LedgerKeyView("key")})
	require.ErrorIs(t, err, refused)
	require.Equal(t, []uint32{pinnedLedger}, core.reads)
}

func TestGetLedgerEntriesStopsWaitingWhenTheContextEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core, key, _ := coreBehindPinnedLedger(t, 1, pinnedLedger-1)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		_, _, err := NewLedgerEntryAtGetter(core, pinnedLedger).GetLedgerEntries(ctx, []xdr.LedgerKeyView{key})
		require.ErrorIs(t, err, errLedgerNotFound)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		// Unpinned reads at 0, 5, 15 and 35 ms; the next would be at 75 ms.
		require.Equal(t, []uint32{pinnedLedger, 0, 0, 0, 0}, core.reads)
	})
}
