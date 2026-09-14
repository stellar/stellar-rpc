//nolint:mnd // percentile numbers are not really magical
package feewindow

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/ledgerbucketwindow"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv1/sqlitedb"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/store"
)

type FeeWindow struct {
	lock          sync.RWMutex
	feesPerLedger *ledgerbucketwindow.LedgerBucketWindow[[]uint64]
	distribution  store.FeeDistribution
}

func NewFeeWindow(retentionWindow uint32) *FeeWindow {
	window := ledgerbucketwindow.NewLedgerBucketWindow[[]uint64](retentionWindow)
	return &FeeWindow{
		feesPerLedger: window,
	}
}

func (fw *FeeWindow) AppendLedgerFees(fees ledgerbucketwindow.LedgerBucket[[]uint64]) error {
	fw.lock.Lock()
	defer fw.lock.Unlock()
	_, err := fw.feesPerLedger.Append(fees)
	if err != nil {
		return err
	}

	var allFees []uint64
	for i := range fw.feesPerLedger.Len() {
		allFees = append(allFees, fw.feesPerLedger.Get(i).BucketContent...)
	}
	fw.distribution = store.ComputeFeeDistribution(allFees, fw.feesPerLedger.Len())

	return nil
}

// Reset clears all ledger fees from the window
func (fw *FeeWindow) Reset() {
	fw.lock.Lock()
	defer fw.lock.Unlock()
	fw.feesPerLedger.Reset()
	fw.distribution = store.FeeDistribution{}
}

func int64ToUint64(value int64, fieldName string) (uint64, error) {
	if value < 0 {
		return 0, errors.New(fieldName + " cannot be negative")
	}
	return uint64(value), nil
}

func (fw *FeeWindow) GetFeeDistribution() store.FeeDistribution {
	fw.lock.RLock()
	defer fw.lock.RUnlock()
	return fw.distribution
}

type FeeWindows struct {
	SorobanInclusionFeeWindow *FeeWindow
	ClassicFeeWindow          *FeeWindow
	networkPassPhrase         string
	db                        *sqlitedb.DB
}

func NewFeeWindows(
	classicRetention uint32, sorobanRetention uint32, networkPassPhrase string, db *sqlitedb.DB,
) *FeeWindows {
	return &FeeWindows{
		SorobanInclusionFeeWindow: NewFeeWindow(sorobanRetention),
		ClassicFeeWindow:          NewFeeWindow(classicRetention),
		networkPassPhrase:         networkPassPhrase,
		db:                        db,
	}
}

//nolint:gocognit,cyclop // one linear pass classifying every fee-bump/tx shape
func (fw *FeeWindows) IngestFees(meta xdr.LedgerCloseMeta) error {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(fw.networkPassPhrase, meta)
	if err != nil {
		return errors.Join(err, fw.db.Rollback())
	}
	var sorobanInclusionFees []uint64
	var classicFees []uint64
	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return errors.Join(err, fw.db.Rollback())
		}
		feeCharged, err := int64ToUint64(int64(tx.Result.Result.FeeCharged), "fee charged")
		if err != nil {
			return errors.Join(err, fw.db.Rollback())
		}
		ops := tx.Envelope.Operations()
		if len(ops) == 0 {
			// should not happen
			continue
		}
		if len(ops) == 1 {
			switch ops[0].Body.Type { //nolint:exhaustive
			case xdr.OperationTypeInvokeHostFunction, xdr.OperationTypeExtendFootprintTtl, xdr.OperationTypeRestoreFootprint:
				var sorobanFees xdr.SorobanTransactionMetaExtV1
				switch tx.UnsafeMeta.V {
				case 3:
					if tx.UnsafeMeta.V3.SorobanMeta == nil || tx.UnsafeMeta.V3.SorobanMeta.Ext.V != 1 {
						continue
					}
					sorobanFees = *tx.UnsafeMeta.V3.SorobanMeta.Ext.V1
				case 4:
					if tx.UnsafeMeta.V4.SorobanMeta == nil || tx.UnsafeMeta.V4.SorobanMeta.Ext.V != 1 {
						continue
					}
					sorobanFees = *tx.UnsafeMeta.V4.SorobanMeta.Ext.V1
				default:
					continue
				}
				resourceFeeCharged := sorobanFees.TotalNonRefundableResourceFeeCharged +
					sorobanFees.TotalRefundableResourceFeeCharged
				resourceFeeChargedUint64, convErr := int64ToUint64(int64(resourceFeeCharged), "resource fee charged")
				if convErr != nil {
					return errors.Join(convErr, fw.db.Rollback())
				}
				inclusionFee := feeCharged - resourceFeeChargedUint64
				sorobanInclusionFees = append(sorobanInclusionFees, inclusionFee)
				continue
			}
		}
		feePerOp := feeCharged / uint64(len(ops))
		classicFees = append(classicFees, feePerOp)
	}
	bucket := ledgerbucketwindow.LedgerBucket[[]uint64]{
		LedgerSeq:            meta.LedgerSequence(),
		LedgerCloseTimestamp: meta.LedgerCloseTime(),
		BucketContent:        classicFees,
	}
	if err := fw.ClassicFeeWindow.AppendLedgerFees(bucket); err != nil {
		return errors.Join(err, fw.db.Rollback())
	}
	bucket.BucketContent = sorobanInclusionFees
	if err := fw.SorobanInclusionFeeWindow.AppendLedgerFees(bucket); err != nil {
		return errors.Join(err, fw.db.Rollback())
	}
	return nil
}

// Reset clears all fee windows
func (fw *FeeWindows) Reset() {
	fw.SorobanInclusionFeeWindow.Reset()
	fw.ClassicFeeWindow.Reset()
}

func (fw *FeeWindows) SorobanInclusionFeeDistribution() store.FeeDistribution {
	return fw.SorobanInclusionFeeWindow.GetFeeDistribution()
}

func (fw *FeeWindows) ClassicFeeDistribution() store.FeeDistribution {
	return fw.ClassicFeeWindow.GetFeeDistribution()
}

// Compile-time check that FeeWindows implements store.FeeStats, the interface
// the shared getFeeStats handler consumes (and the contract v2's fee windows
// must satisfy). Breaks the build here, next to the type, if the two drift.
var _ store.FeeStats = &FeeWindows{}

func (fw *FeeWindows) AsMigration(seqRange sqlitedb.LedgerSeqRange) sqlitedb.Migration {
	return &feeWindowMigration{
		firstLedger: seqRange.First,
		lastLedger:  seqRange.Last,
		windows:     fw,
	}
}

type feeWindowMigration struct {
	firstLedger uint32
	lastLedger  uint32
	windows     *FeeWindows
}

func (fw *feeWindowMigration) ApplicableRange() sqlitedb.LedgerSeqRange {
	return sqlitedb.LedgerSeqRange{
		First: fw.firstLedger,
		Last:  fw.lastLedger,
	}
}

func (fw *feeWindowMigration) Apply(_ context.Context, meta xdr.LedgerCloseMeta) error {
	return fw.windows.IngestFees(meta)
}

func (fw *feeWindowMigration) Commit(_ context.Context) error {
	return nil // no-op
}

// ensure we conform to the migration interface
var _ sqlitedb.Migration = &feeWindowMigration{}
