package serve

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/events"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/catalog"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/geometry"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/eventstore"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/storage/stores/hotchunk"
)

// contractEventXDR builds a contract-type ContractEvent for the given contract
// ID with a single symbol topic and symbol data — the minimal shape the events
// index and the protocol filter both key off (mirror of the eventstore and
// ingest test fixtures).
func contractEventXDR(t *testing.T, contractID xdr.ContractId, topic, data string) xdr.ContractEvent {
	t.Helper()
	topicSym := xdr.ScSymbol(topic)
	dataSym := xdr.ScSymbol(data)
	cid := contractID
	ev := xdr.ContractEvent{
		ContractId: &cid,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: []xdr.ScVal{{Type: xdr.ScValTypeScvSymbol, Sym: &topicSym}},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &dataSym},
			},
		},
	}
	return ev
}

// eventLedgerLCM builds a V2 LedgerCloseMeta carrying one transaction whose sole
// operation emits evs (operation-level contract events). It is the hot-ingest
// counterpart of writeColdEventsChunk: IngestLedger runs ExtractLedgerEvents over
// this LCM, so the events land in the chunk's events store in cursor order. The
// header close time is seq*10 so a test can prove the served close time came from
// the ingested bytes.
func eventLedgerLCM(t *testing.T, seq uint32, evs []xdr.ContractEvent) []byte {
	t.Helper()
	meta := xdr.TransactionMeta{
		V:  4,
		V4: &xdr.TransactionMetaV4{Operations: []xdr.OperationMetaV2{{Events: evs}}},
	}
	src := xdr.MustMuxedAddress(keypair.MustRandom().Address())
	envelope := xdr.TransactionEnvelope{
		Type: xdr.EnvelopeTypeEnvelopeTypeTx,
		V1: &xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				SourceAccount: src,
				SeqNum:        xdr.SequenceNumber(seq),
				Ext:           xdr.TransactionExt{V: 1, SorobanData: &xdr.SorobanTransactionData{}},
			},
		},
	}
	hash, err := network.HashTransactionInEnvelope(envelope, network.PublicNetworkPassphrase)
	require.NoError(t, err)
	comp := []xdr.TxSetComponent{{
		Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
		TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
			Txs: []xdr.TransactionEnvelope{envelope},
		},
	}}
	lcm := xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue:  xdr.StellarValue{CloseTime: xdr.TimePoint(uint64(seq) * 10)},
					LedgerSeq: xdr.Uint32(seq),
				},
			},
			TxSet: xdr.GeneralizedTransactionSet{
				V:       1,
				V1TxSet: &xdr.TransactionSetV1{Phases: []xdr.TransactionPhase{{V: 0, V0Components: &comp}}},
			},
			TxProcessing: []xdr.TransactionResultMetaV1{{
				TxApplyProcessing: meta,
				Result: xdr.TransactionResultPair{
					TransactionHash: hash,
					Result: xdr.TransactionResult{
						FeeCharged: 100,
						Result: xdr.TransactionResultResult{
							Code:    xdr.TransactionResultCodeTxSuccess,
							Results: &[]xdr.OperationResult{},
						},
					},
				},
			}},
		},
	}
	raw, err := lcm.MarshalBinary()
	require.NoError(t, err)
	return raw
}

// openHotEventChunk runs the real create bracket for chunk c and ingests one
// event-bearing ledger per entry in ledgerEvents (keyed by sequence), returning
// the live write handle. Mirrors registry_test's openHotChunk but drives
// event-carrying LCMs so the chunk's events store is populated.
func openHotEventChunk(
	t *testing.T, cat *catalog.Catalog, layout geometry.Layout, c chunk.ID,
	seqs []uint32, ledgerEvents map[uint32][]xdr.ContractEvent,
) *hotchunk.DB {
	t.Helper()
	require.NoError(t, cat.BeginHotCreate(c))
	db, err := hotchunk.Open(layout.HotChunkPath(c), c, silentLogger())
	require.NoError(t, err)
	for _, s := range seqs {
		raw := eventLedgerLCM(t, s, ledgerEvents[s])
		_, err := db.IngestLedger(s, xdr.LedgerCloseMetaView(raw))
		require.NoError(t, err)
	}
	require.NoError(t, cat.FinishHotCreate(c))
	return db
}

// writeColdEventsChunk materializes cold events artifacts (events.pack + index)
// for chunk c at layout.EventsBucketDir(c), holding the contract events in
// ledgerEvents. offsets start at startLedger and cover the contiguous run
// [startLedger, startLedger+len(seqs)-1] (a partial-chunk pack — the POC fixture
// keeps cold artifacts tiny, like writeColdLedgerRun). Payloads carry a
// deterministic close time (seq*10) matching the hot path's header close time.
//
// poison, when non-nil, runs over the term bitmaps just before the index is
// written — a test hook to inject false-positive event IDs, simulating what an
// xxh3_128 term collision would produce (mirror of eventstore's
// TestQuery_PostFilterRejectsTermHashCollision technique on the cold side).
func writeColdEventsChunk(
	t *testing.T, layout geometry.Layout, c chunk.ID,
	startLedger uint32, seqs []uint32, ledgerEvents map[uint32][]xdr.ContractEvent,
	poison func(events.Bitmaps),
) {
	t.Helper()
	dir := layout.EventsBucketDir(c)
	require.NoError(t, os.MkdirAll(dir, 0o755))

	cw, err := eventstore.NewColdWriter(c, dir, eventstore.ColdWriterOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = cw.Close() })

	offsets := events.NewLedgerOffsets(startLedger)
	idx := events.NewBitmaps()
	eventID := uint32(0)
	for _, seq := range seqs {
		evs := ledgerEvents[seq]
		require.NoError(t, offsets.Append(seq, uint32(len(evs))))
		var txHash xdr.Hash
		txHash[0] = byte(seq)
		txHash[1] = byte(seq >> 8)
		for evIdx, ev := range evs {
			raw, err := ev.MarshalBinary()
			require.NoError(t, err)
			p := events.Payload{
				TxHash:             txHash,
				LedgerSequence:     seq,
				TxIdx:              1,
				OpIdx:              0,
				LedgerClosedAt:     int64(seq) * 10,
				EventIdx:           uint32(evIdx),
				ContractEventBytes: raw,
			}
			require.NoError(t, cw.Append(p))
			keys, err := events.TermsForBytes(raw)
			require.NoError(t, err)
			for _, k := range keys {
				idx.AddTo(k, eventID)
			}
			eventID++
		}
	}
	require.NoError(t, cw.Finish(offsets))
	if poison != nil {
		poison(idx)
	}
	require.NoError(t, eventstore.WriteColdIndex(context.Background(), c, idx, dir))
}

// freezeEvents marks chunk c's events artifact frozen in the catalog, so the
// View resolves it to the cold events reader (cold wins over the hot handle).
func freezeEvents(t *testing.T, cat *catalog.Catalog, c chunk.ID) {
	t.Helper()
	require.NoError(t, cat.MarkChunkFreezing(c, geometry.KindEvents))
	require.NoError(t, cat.FlipChunkFrozen(c, geometry.KindEvents))
}
