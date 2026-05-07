// Package format renders byte counts and integer counts as
// human-readable strings for log lines that an operator will read.
// Add helpers here when a real call site appears, not before.
//
// Endianness for RocksDB-stored integers is NOT a concern of this
// package — see pkg/rocksdb's EncodeUint32 / DecodeUint32 helpers.
package format

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Bytes formats a byte count as a human-readable disk-space string
// using decimal (1000-based) SI units — "1.50 KB", "2.00 GB", etc.
// Matches the convention used by `df`, disk vendor specs, and most
// storage telemetry; an operator reading the log can compare a WAL
// size or total SST size directly against `df -h` output.
// For values below 1 KB, returns the raw count with a "B" suffix.
func Bytes(bytes int64) string {
	const unit = 1000
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// Duration formats a time.Duration as a human-readable string,
// picking unit precision that fits the magnitude.
// Negative durations carry a leading "-".
//
// Below one minute, fractional seconds are allowed (the unit suffix
// disambiguates).
// At and above one minute, every level uses whole integer seconds in
// "Xm Ys" / "Xh Ym Zs" / "Xd Yh Zm" / "Xy Ymo Zd" form — no
// fractional component anywhere, no chance of misreading "2.5" as
// "2 minutes 30 seconds".
//
//   - sub-µs:    "500ns"
//   - sub-ms:    "456µs"
//   - sub-1s:    "1.5ms" / "123.456ms"
//   - sub-1min:  "0.5s" / "45.67s"
//   - <1h:       "2m 5s" / "3m 45s"
//   - <1day:     "2h 30m 15s"
//   - <1y:       "5d 12h 30m"
//   - else:      "2y 3mo 15d"
func Duration(d time.Duration) string {
	if d < 0 {
		return "-" + Duration(-d)
	}
	switch {
	case d == 0:
		return "0s"
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%dµs", d.Microseconds())
	case d < time.Second:
		return trimZeros(fmt.Sprintf("%.3f", float64(d.Nanoseconds())/float64(time.Millisecond))) + "ms"
	case d < time.Minute:
		return trimZeros(fmt.Sprintf("%.2f", float64(d.Nanoseconds())/float64(time.Second))) + "s"
	case d < time.Hour:
		return durationMinutes(d)
	case d < 24*time.Hour:
		return durationHours(d)
	case d < 365*24*time.Hour:
		return durationDays(d)
	default:
		return durationYears(d)
	}
}

// trimZeros strips trailing zeros and a trailing decimal point from a
// fixed-precision float string ("1.500" → "1.5", "1.000" → "1").
func trimZeros(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	s = strings.TrimRight(s, "0")
	return strings.TrimRight(s, ".")
}

func durationMinutes(d time.Duration) string {
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60
	if secs == 0 {
		return fmt.Sprintf("%dm", mins)
	}
	return fmt.Sprintf("%dm %ds", mins, secs)
}

func durationHours(d time.Duration) string {
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	secs := int(d.Seconds()) % 60
	switch {
	case mins == 0 && secs == 0:
		return fmt.Sprintf("%dh", hours)
	case secs == 0:
		return fmt.Sprintf("%dh %dm", hours, mins)
	default:
		return fmt.Sprintf("%dh %dm %ds", hours, mins, secs)
	}
}

func durationDays(d time.Duration) string {
	const day = 24 * time.Hour
	days := int(d / day)
	remaining := d % day
	hours := int(remaining.Hours())
	mins := int(remaining.Minutes()) % 60
	switch {
	case hours == 0 && mins == 0:
		return fmt.Sprintf("%dd", days)
	case mins == 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	default:
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
}

func durationYears(d time.Duration) string {
	const day = 24 * time.Hour
	const year = 365 * day
	month := 30 * day // approximate
	years := int(d / year)
	remaining := d % year
	months := int(remaining / month)
	days := int((remaining % month) / day)
	switch {
	case months == 0 && days == 0:
		return fmt.Sprintf("%dy", years)
	case days == 0:
		return fmt.Sprintf("%dy %dmo", years, months)
	default:
		return fmt.Sprintf("%dy %dmo %dd", years, months, days)
	}
}

// Number formats an int64 with thousands separators ("1,234,567").
// Negative numbers carry a leading "-".
func Number(n int64) string {
	if n < 0 {
		return "-" + Number(-n)
	}
	if n < 1000 {
		return strconv.FormatInt(n, 10)
	}
	s := strconv.FormatInt(n, 10)
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
