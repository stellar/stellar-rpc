package integrationtest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	client "github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/integrationtest/infrastructure"
)

func testGetLedgers(t *testing.T, client *client.Client) {
	// Wait until there's at least 10 ledgers
	var ledgerCount uint
	var oldestLedger uint32

	for ledgerCount < 5 {
		health, err := client.GetHealth(t.Context())
		require.NoError(t, err)

		ledgerCount = uint(health.LatestLedger) - uint(health.OldestLedger) + 1
		oldestLedger = health.OldestLedger

		time.Sleep(time.Second)
	}

	// Get first group of ledgers
	request := protocol.GetLedgersRequest{
		StartLedger: oldestLedger,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 5,
		},
	}

	result, err := client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 5)
	prevLedgers := result.Ledgers

	// Get ledgers using previous result's cursor
	request = protocol.GetLedgersRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Cursor: result.Cursor,
			Limit:  8,
		},
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.NotEmpty(t, result.Ledgers)
	require.LessOrEqual(t, len(result.Ledgers), 8)
	require.Equal(t, prevLedgers[len(prevLedgers)-1].Sequence+1, result.Ledgers[0].Sequence)

	// Test with JSON format
	request = protocol.GetLedgersRequest{
		StartLedger: oldestLedger + 1,
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: 1,
		},
		Format: protocol.FormatJSON,
	}
	result, err = client.GetLedgers(t.Context(), request)
	require.NoError(t, err)
	require.Len(t, result.Ledgers, 1)
	require.NotEmpty(t, result.Ledgers[0].LedgerHeaderJSON)
	require.NotEmpty(t, result.Ledgers[0].LedgerMetadataJSON)

	// Test invalid requests
	invalidRequests := []protocol.GetLedgersRequest{
		{StartLedger: result.OldestLedger - 4}, // -3 to exceed data store
		{StartLedger: result.LatestLedger + 1},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Cursor: "invalid",
			},
		},
		{
			Pagination: &protocol.LedgerPaginationOptions{
				Limit: 100_000,
			},
		},
	}

	for _, req := range invalidRequests {
		_, err = client.GetLedgers(t.Context(), req)
		require.Error(t, err, "request: %+v (oldest: %d, latest: %d)",
			req, result.OldestLedger, result.LatestLedger)
	}
}

func TestGetLedgers(t *testing.T) {
	test := infrastructure.NewTest(t, nil)
	client := test.GetRPCLient()
	testGetLedgers(t, client)
}

func TestGetLedgersFromDatastore(t *testing.T) {
	gcsSetup := infrastructure.NewGCSTestSetup(t, infrastructure.DefaultGCSTestConfig())
	defer gcsSetup.Stop()

	// add files to GCS for ledgers 35-40
	gcsSetup.AddLedgers(35, 40)

	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
		DatastoreConfigFunc: gcsSetup.DatastoreConfigFunc(),
		NoParallel:          true, // can't use parallel due to env vars
	})
	cl := test.GetRPCLient() // at this point we're at like ledger 30

	waitUntil := func(cond func(h protocol.GetHealthResponse) bool, timeout time.Duration) protocol.GetHealthResponse {
		var last protocol.GetHealthResponse
		require.Eventually(t, func() bool {
			resp, err := cl.GetHealth(t.Context())
			require.NoError(t, err)
			last = resp
			return cond(resp)
		}, timeout, 100*time.Millisecond, "last health: %+v", last)
		return last
	}

	getSeqs := func(resp protocol.GetLedgersResponse) []uint32 {
		out := make([]uint32, len(resp.Ledgers))
		for i, l := range resp.Ledgers {
			out[i] = l.Sequence
		}
		return out
	}

	request := func(start uint32, limit uint, cursor string) (protocol.GetLedgersResponse, error) {
		req := protocol.GetLedgersRequest{
			StartLedger: start,
			Pagination: &protocol.LedgerPaginationOptions{
				Limit:  limit,
				Cursor: cursor,
			},
		}
		return cl.GetLedgers(t.Context(), req)
	}

	// ensure oldest > 40 so datastore set ([35..40]) is below local window
	health := waitUntil(func(h protocol.GetHealthResponse) bool {
		return uint(h.OldestLedger) > 40
	}, 30*time.Second)

	oldest := health.OldestLedger
	latest := health.LatestLedger
	require.Greater(t, oldest, uint32(40), "precondition: oldest must be > 40")
	require.GreaterOrEqual(t, latest, oldest)

	// --- 1) datastore-only: entirely below oldest ---
	t.Run("datastore_only", func(t *testing.T) {
		res, err := request(35, 3, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 3)
		require.Equal(t, []uint32{35, 36, 37}, getSeqs(res))
	})

	// --- 2) local-only: entirely at/above oldest ---
	t.Run("local_only", func(t *testing.T) {
		start := oldest
		limit := 3
		res, err := request(start, uint(limit), "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 3)
	})

	// --- 3) mixed: cross boundary (datastore then local) ---
	t.Run("mixed_datastore_and_local", func(t *testing.T) {
		// 39,40 from datastore; 41,42 from local
		require.GreaterOrEqual(t, latest, uint32(42), "need latest >= 42")
		res, err := request(39, 4, "")
		require.NoError(t, err)
		require.Len(t, res.Ledgers, 4)
		require.Equal(t, []uint32{39, 40, 41, 42}, getSeqs(res))

		// verify cursor continuity across boundary
		next, err := request(0, 2, res.Cursor)
		require.NoError(t, err)
		if len(next.Ledgers) > 0 {
			require.EqualValues(t, 43, next.Ledgers[0].Sequence)
		}
	})

	// --- 4) negative: below datastore floor (not available anywhere) ---
	t.Run("negative_below_datastore_floor", func(t *testing.T) {
		res, err := request(2, 3, "")
		// accept either an error or an empty page; but never data
		if err != nil {
			return
		}
		require.Empty(t, res.Ledgers, "expected no ledgers when requesting below datastore floor")
	})

	// --- 5) negative: beyond latest ---
	t.Run("negative_beyond_latest", func(t *testing.T) {
		res, err := request(latest+1, 1, "")
		if err != nil {
			return
		}
		require.Empty(t, res.Ledgers, "expected no ledgers when requesting beyond latest")
	})
}
