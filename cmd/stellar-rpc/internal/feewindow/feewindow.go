//nolint:mnd // percentile numbers are not really magical
package feewindow

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/db"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/ledgerbucketwindow"
)

type FeeDistribution struct {
	Max         uint64
	Min         uint64
	Mode        uint64
	P10         uint64
	P20         uint64
	P30         uint64
	P40         uint64
	P50         uint64
	P60         uint64
	P70         uint64
	P80         uint64
	P90         uint64
	P95         uint64
	P99         uint64
	FeeCount    uint32
	LedgerCount uint32
}

type FeeWindow struct {
	lock          sync.RWMutex
	feesPerLedger *ledgerbucketwindow.LedgerBucketWindow[[]uint64]
	distribution  FeeDistribution
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
	fw.distribution = computeFeeDistribution(allFees, fw.feesPerLedger.Len())

	return nil
}

// Reset clears all ledger fees from the window
func (fw *FeeWindow) Reset() {
	fw.lock.Lock()
	defer fw.lock.Unlock()
	fw.feesPerLedger.Reset()
	fw.distribution = FeeDistribution{}
}

func computeFeeDistribution(fees []uint64, ledgerCount uint32) FeeDistribution {
	if len(fees) == 0 {
		return FeeDistribution{}
	}
	slices.Sort(fees)
	mode := fees[0]
	lastVal := fees[0]
	maxRepetitions := 0
	localRepetitions := 0
	for i := 1; i < len(fees); i++ {
		if fees[i] == lastVal {
			localRepetitions++
			continue
		}

		// new cluster of values

		if localRepetitions > maxRepetitions {
			maxRepetitions = localRepetitions
			mode = lastVal
		}
		lastVal = fees[i]
		localRepetitions = 0
	}

	if localRepetitions > maxRepetitions {
		// the last cluster of values was the longest
		mode = fees[len(fees)-1]
	}

	count := len(fees)
	// nearest-rank percentile
	percentile := func(p uint64) uint64 {
		// ceiling(p*count/100)
		kth := ((p * uint64(count)) + 100 - 1) / 100
		return fees[kth-1]
	}
	return FeeDistribution{
		Max:         fees[len(fees)-1],
		Min:         fees[0],
		Mode:        mode,
		P10:         percentile(10),
		P20:         percentile(20),
		P30:         percentile(30),
		P40:         percentile(40),
		P50:         percentile(50),
		P60:         percentile(60),
		P70:         percentile(70),
		P80:         percentile(80),
		P90:         percentile(90),
		P95:         percentile(95),
		P99:         percentile(99),
		FeeCount:    uint32(count),
		LedgerCount: ledgerCount,
	}
}

func (fw *FeeWindow) GetFeeDistribution() FeeDistribution {
	fw.lock.RLock()
	defer fw.lock.RUnlock()
	return fw.distribution
}

type FeeWindows struct {
	SorobanInclusionFeeWindow *FeeWindow
	ClassicFeeWindow          *FeeWindow
	networkPassPhrase         string
	db                        *db.DB
}

func NewFeeWindows(classicRetention uint32, sorobanRetention uint32, networkPassPhrase string, db *db.DB) *FeeWindows {
	return &FeeWindows{
		SorobanInclusionFeeWindow: NewFeeWindow(sorobanRetention),
		ClassicFeeWindow:          NewFeeWindow(classicRetention),
		networkPassPhrase:         networkPassPhrase,
		db:                        db,
	}
}

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
		feeCharged := uint64(tx.Result.Result.FeeCharged)
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
					fees, ok := extractSorobanFeesForXdrTransactionMetaV5(tx.UnsafeMeta)
					if !ok {
						continue
					}
					sorobanFees = fees
				}
				resourceFeeCharged := sorobanFees.TotalNonRefundableResourceFeeCharged +
					sorobanFees.TotalRefundableResourceFeeCharged
				inclusionFee := feeCharged - uint64(resourceFeeCharged)
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

func (fw *FeeWindows) AsMigration(seqRange db.LedgerSeqRange) db.Migration {
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

func (fw *feeWindowMigration) ApplicableRange() db.LedgerSeqRange {
	return db.LedgerSeqRange{
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
var _ db.Migration = &feeWindowMigration{}
