package event

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/rocksdb"
)

// BenchmarkEventIndex_10M builds the hot index at full chunk scale and
// measures the heap it holds once built.
// Distribution modeled on real production chunk data:
//
//	chunk       events       terms     total_adds   mean_card   max_card
//	005901     8,941,737   2,595,814     37,599,602       14.5    5,980,086
//	005903     9,243,803   2,638,816     38,622,331       14.6    6,350,706
//	005908     9,255,090   2,289,828     37,397,684       16.3    6,440,193
func BenchmarkEventIndex_10M(b *testing.B) {
	for b.Loop() {
		store, err := rocksdb.New(rocksdb.Config{
			Path:           b.TempDir(),
			ColumnFamilies: CFNames(),
			Logger:         silentLogger(),
			Tuning:         rocksdb.Tuning{BlockCacheMB: 64},
			PerCFOptions:   CFOptions(),
		})
		require.NoError(b, err)
		idx := newHotIndex(store, 0)

		start := time.Now()
		buildIndex10M(b, idx)
		require.NoError(b, idx.settle())
		buildSec := time.Since(start).Seconds()

		b.ReportMetric(buildSec, "build_sec")
		sealed, err := sealedHotSlabs(store)
		require.NoError(b, err)
		b.ReportMetric(float64(sealed), "sealed_slabs")

		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		b.ReportMetric(float64(mem.HeapInuse)/(1024*1024), "heap_MB")

		runtime.KeepAlive(idx)
		require.NoError(b, store.Close())
	}
}

// buildIndex10M simulates a full chunk based on real production data:
// ~9M events, ~2M unique terms, ~35M adds for the contract-ID and topic
// terms, plus the 2 adds per event the type and topic-count families
// contribute. It feeds the index one ledger at a time, as ingestion does.
func buildIndex10M(b *testing.B, idx *hotIndex) {
	const (
		totalEvents     = 9_000_000
		numContracts    = 10_000
		numTopicVals    = 3_000_000
		eventsPerLedger = 900
	)

	rng := rand.New(rand.NewSource(42))

	contractKeys := make([]TermKey, numContracts)
	for i := range contractKeys {
		contractKeys[i] = ComputeTermKey(fmt.Appendf(nil, "contract-%d", i), FieldContractID)
	}

	topicKeys := make([]TermKey, numTopicVals)
	for i := range topicKeys {
		topicKeys[i] = ComputeTermKey(fmt.Appendf(nil, "topic-%d", i), Field(1+i%4))
	}

	zipf := rand.NewZipf(rng, 1.01, 1.0, uint64(numTopicVals-1))

	// Type is near-degenerate in real data: system events exist only on
	// contract upgrades, so one bitmap is near-full and the other
	// near-empty. Topic count spreads the chunk across every bucket,
	// including the empty-topic and overflow ones, since how that family
	// partitions decides whether it is cheap or not.
	const systemEventEvery = 100_000
	contractType := EventTypeTermKey(xdr.ContractEventTypeContract)
	systemType := EventTypeTermKey(xdr.ContractEventTypeSystem)

	ledger := make([][]TermKey, 0, eventsPerLedger)
	for eventID := range uint32(totalEvents) {
		keys := make([]TermKey, 0, maxTermsPerEvent)
		keys = append(keys, contractKeys[eventID%uint32(numContracts)])
		numTopics := rng.Intn(protocol.MaxTopicCount + 2)
		for range min(numTopics, protocol.MaxTopicCount) {
			keys = append(keys, topicKeys[zipf.Uint64()])
		}
		if eventID%systemEventEvery == 0 {
			keys = append(keys, systemType)
		} else {
			keys = append(keys, contractType)
		}
		keys = append(keys, TopicCountTermKey(numTopics))
		ledger = append(ledger, keys)
		if len(ledger) == eventsPerLedger {
			require.NoError(b, idx.add(eventID+1-eventsPerLedger, ledger))
			ledger = ledger[:0]
		}
	}
}
