package bench

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRecordPeakRSSObserved: a successful read records peak_rss_bytes with the
// byte count stored unchanged in the duration column (total_ns == the bytes,
// one sample).
func TestRecordPeakRSSObserved(t *testing.T) {
	const wantBytes = 4 * 1024 * 1024 * 1024 // 4 GiB

	sink := newCSVSink()
	recordPeakRSS(testLogger(), sink, func() (uint64, error) { return wantBytes, nil })

	outDir := t.TempDir()
	_, err := sink.writeCSVs(outDir)
	require.NoError(t, err)

	driver := readCSV(t, filepath.Join(outDir, "driver.csv"))
	require.Contains(t, driver, "peak_rss_bytes")
	assert.EqualValues(t, 1, driver["peak_rss_bytes"]["n"])
	assert.EqualValues(t, wantBytes, driver["peak_rss_bytes"]["total_ns"])
	assert.EqualValues(t, wantBytes, driver["peak_rss_bytes"]["max_ns"])
}

// TestRecordPeakRSSSkippedOnError: a failed read (procfs off Linux) skips the
// row without failing — the sink writes nothing.
func TestRecordPeakRSSSkippedOnError(t *testing.T) {
	sink := newCSVSink()
	recordPeakRSS(testLogger(), sink, func() (uint64, error) { return 0, errors.New("no /proc") })

	written, err := sink.writeCSVs(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, written)
}
