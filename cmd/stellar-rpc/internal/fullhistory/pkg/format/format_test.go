package format

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Duration must handle time.Duration(math.MinInt64) without infinite
// recursion.
// Without the MinInt64 guard at the top of Duration, `-d` overflows
// two's-complement int64 back to MinInt64 and the function recurses
// until the goroutine stack is exhausted (a real stack overflow,
// not a test-time error).
// Reported on PR #733; this is the regression test that pins the fix.
func TestDuration_MinInt64_DoesNotStackOverflow(t *testing.T) {
	s := Duration(time.Duration(math.MinInt64))
	// Leading "-" + non-empty body proves the call returned cleanly
	// (rather than crashing the goroutine).
	assert.NotEmpty(t, s)
	assert.Equal(t, byte('-'), s[0])
}

// Duration formats a normal negative value with a leading "-" and the
// same body it would produce for the absolute value.
func TestDuration_NegativeFormatsWithLeadingMinus(t *testing.T) {
	assert.Equal(t, "-2h", Duration(-2*time.Hour))
	assert.Equal(t, "-45.67s", Duration(-45_670*time.Millisecond))
}
