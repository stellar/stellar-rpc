package infrastructure

import (
	"bytes"
	"path"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/support/compressxdr"
	"github.com/stellar/go-stellar-sdk/support/datastore"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/config"
)

// GCSTestSetup holds the resources for a fake GCS server used in tests.
type GCSTestSetup struct {
	Server     *fakestorage.Server
	BucketName string
	Prefix     string
	Schema     datastore.DataStoreSchema
}

// GCSTestConfig provides options for setting up the fake GCS server.
type GCSTestConfig struct {
	BucketName        string
	Prefix            string
	FilesPerPartition uint32
	LedgersPerFile    uint32
}

// DefaultGCSTestConfig returns a default GCS test configuration.
func DefaultGCSTestConfig() GCSTestConfig {
	return GCSTestConfig{
		BucketName:        "test-bucket",
		Prefix:            "ledgers",
		FilesPerPartition: 1,
		LedgersPerFile:    1,
	}
}

// NewGCSTestSetup creates a fake GCS server and bucket for testing.
// It sets the STORAGE_EMULATOR_HOST environment variable.
// The caller is responsible for calling Stop() on the returned GCSTestSetup.Server.
func NewGCSTestSetup(t *testing.T, cfg GCSTestConfig) *GCSTestSetup {
	t.Helper()

	opts := fakestorage.Options{
		Scheme:     "http",
		Host:       "127.0.0.1",
		PublicHost: "127.0.0.1",
	}
	gcsServer, err := fakestorage.NewServerWithOptions(opts)
	require.NoError(t, err)

	t.Setenv("STORAGE_EMULATOR_HOST", gcsServer.URL())
	gcsServer.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: cfg.BucketName})

	schema := datastore.DataStoreSchema{
		FilesPerPartition: cfg.FilesPerPartition,
		LedgersPerFile:    cfg.LedgersPerFile,
	}

	return &GCSTestSetup{
		Server:     gcsServer,
		BucketName: cfg.BucketName,
		Prefix:     cfg.Prefix,
		Schema:     schema,
	}
}

// Stop stops the fake GCS server.
func (g *GCSTestSetup) Stop() {
	g.Server.Stop()
}

// AddLedgers adds ledger objects to the fake GCS bucket for the given sequence range.
func (g *GCSTestSetup) AddLedgers(startSeq, endSeq uint32) {
	for seq := startSeq; seq <= endSeq; seq++ {
		g.Server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: g.BucketName,
				Name:       path.Join(g.Prefix, g.Schema.GetObjectKeyFromSequenceNumber(seq)),
			},
			Content: CreateLCMBatchBuffer(seq),
		})
	}
}

// DatastoreConfigFunc returns a function that configures the RPC config to use the fake GCS datastore.
func (g *GCSTestSetup) DatastoreConfigFunc() func(cfg *config.Config) {
	return func(cfg *config.Config) {
		cfg.ServeLedgersFromDatastore = true
		cfg.BufferedStorageBackendConfig = ledgerbackend.BufferedStorageBackendConfig{
			BufferSize: 15,
			NumWorkers: 2,
		}
		cfg.DataStoreConfig = datastore.DataStoreConfig{
			Type:   "GCS",
			Params: map[string]string{"destination_bucket_path": path.Join(g.BucketName, g.Prefix)},
			Schema: g.Schema,
		}
		// reduce retention windows to force usage of datastore
		cfg.HistoryRetentionWindow = 15
		cfg.ClassicFeeStatsLedgerRetentionWindow = 15
		cfg.SorobanFeeStatsLedgerRetentionWindow = 15
	}
}

// CreateLCMBatchBuffer creates a compressed XDR LedgerCloseMetaBatch for the given sequence.
func CreateLCMBatchBuffer(seq uint32) []byte {
	lcm := xdr.LedgerCloseMetaBatch{
		StartSequence:    xdr.Uint32(seq),
		EndSequence:      xdr.Uint32(seq),
		LedgerCloseMetas: []xdr.LedgerCloseMeta{createTestLedger(seq)},
	}

	var buf bytes.Buffer
	encoder := compressxdr.NewXDREncoder(compressxdr.DefaultCompressor, lcm)
	_, _ = encoder.WriteTo(&buf)

	return buf.Bytes()
}

func createTestLedger(sequence uint32) xdr.LedgerCloseMeta {
	// Build the transaction envelope (inlined txEnvelope)
	envelope, err := xdr.NewTransactionEnvelope(
		xdr.EnvelopeTypeEnvelopeTypeTx,
		xdr.TransactionV1Envelope{
			Tx: xdr.Transaction{
				Fee:    1,
				SeqNum: xdr.SequenceNumber(sequence),
				SourceAccount: xdr.MustMuxedAddress(
					"MA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVAAAAAAAAAAAAAJLK",
				),
				Ext: xdr.TransactionExt{
					V:           1,
					SorobanData: &xdr.SorobanTransactionData{},
				},
			},
		},
	)
	if err != nil {
		panic(err)
	}

	// compute tx hash
	hash, err := network.HashTransactionInEnvelope(envelope, StandaloneNetworkPassphrase)
	if err != nil {
		panic(err)
	}

	// build transaction result
	makeResult := func(successful bool) xdr.TransactionResult {
		code := xdr.TransactionResultCodeTxBadSeq
		if successful {
			code = xdr.TransactionResultCodeTxSuccess
		}
		opResults := []xdr.OperationResult{}
		return xdr.TransactionResult{
			FeeCharged: 100,
			Result: xdr.TransactionResultResult{
				Code:    code,
				Results: &opResults,
			},
		}
	}

	// build TxProcessing entry
	makeTxMeta := func(res xdr.TransactionResult) xdr.TransactionResultMetaV1 {
		return xdr.TransactionResultMetaV1{
			TxApplyProcessing: xdr.TransactionMeta{
				V:          3,
				Operations: &[]xdr.OperationMeta{},
				V3:         &xdr.TransactionMetaV3{},
			},
			Result: xdr.TransactionResultPair{
				TransactionHash: hash,
				Result:          res,
			},
		}
	}

	// Two transactions: one successful, one failed
	txProcessing := []xdr.TransactionResultMetaV1{
		makeTxMeta(makeResult(true)),
		makeTxMeta(makeResult(false)),
	}

	return xdr.LedgerCloseMeta{
		V: 2,
		V2: &xdr.LedgerCloseMetaV2{
			LedgerHeader: xdr.LedgerHeaderHistoryEntry{
				Header: xdr.LedgerHeader{
					ScpValue: xdr.StellarValue{
						CloseTime: xdr.TimePoint(int64(sequence+100)*25 + 100),
					},
					LedgerSeq: xdr.Uint32(sequence),
				},
			},
			TxProcessing: txProcessing,
			TxSet: xdr.GeneralizedTransactionSet{
				V: 1,
				V1TxSet: &xdr.TransactionSetV1{
					PreviousLedgerHash: xdr.Hash{1},
					Phases: []xdr.TransactionPhase{
						{
							V: 0,
							V0Components: &[]xdr.TxSetComponent{
								{
									Type: xdr.TxSetComponentTypeTxsetCompTxsMaybeDiscountedFee,
									TxsMaybeDiscountedFee: &xdr.TxSetComponentTxsMaybeDiscountedFee{
										BaseFee: nil,
										Txs: []xdr.TransactionEnvelope{
											envelope,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}
