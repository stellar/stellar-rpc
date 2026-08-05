package events

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// BenchmarkEventIndex_10M measures heap at full chunk scale.
// Distribution modeled on real production chunk data:
//
//	chunk       events       terms     total_adds   mean_card   max_card
//	005901     8,941,737   2,595,814     37,599,602       14.5    5,980,086
//	005903     9,243,803   2,638,816     38,622,331       14.6    6,350,706
//	005908     9,255,090   2,289,828     37,397,684       16.3    6,440,193
func BenchmarkEventIndex_10M(b *testing.B) {
	for b.Loop() {
		start := time.Now()
		idx := buildIndex10M()
		buildSec := time.Since(start).Seconds()

		b.ReportMetric(buildSec, "build_sec")
		b.ReportMetric(float64(len(idx.terms)), "terms")

		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		b.ReportMetric(float64(mem.HeapInuse)/(1024*1024), "heap_MB")

		runtime.KeepAlive(idx)
	}
}

// buildIndex10M simulates a full chunk based on real production data:
// ~9M events, ~2M unique terms, ~35M adds for the contract-ID and topic
// terms, plus the 2 adds per event the type and topic-count families
// contribute.
//
// Adds are grouped by term within a ledger, the way HotStore.applyLedger
// batches them. Feeding a chunk-wide term one event at a time instead
// pays ConcurrentBitmaps.AddTo's COW clone per event and costs several
// times the build time, which no production path does.
func buildIndex10M() *ConcurrentBitmaps {
	const (
		totalEvents     = 9_000_000
		numContracts    = 10_000
		numTopicVals    = 3_000_000
		eventsPerLedger = 900
	)

	idx := NewConcurrentBitmapsFromBitmaps(NewBitmaps())
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

	perKeyIDs := make(map[TermKey][]uint32, 64)
	flush := func() {
		for key, ids := range perKeyIDs {
			idx.AddTo(key, ids...)
			delete(perKeyIDs, key)
		}
	}
	add := func(key TermKey, eventID uint32) {
		perKeyIDs[key] = append(perKeyIDs[key], eventID)
	}

	for eventID := range uint32(totalEvents) {
		add(contractKeys[eventID%uint32(numContracts)], eventID)
		numTopics := rng.Intn(protocol.MaxTopicCount + 2)
		for range numTopics {
			add(topicKeys[zipf.Uint64()], eventID)
		}
		if eventID%systemEventEvery == 0 {
			add(systemType, eventID)
		} else {
			add(contractType, eventID)
		}
		add(TopicCountTermKey(numTopics), eventID)
		if (eventID+1)%eventsPerLedger == 0 {
			flush()
		}
	}
	flush()

	return idx
}
