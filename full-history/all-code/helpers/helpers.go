package helpers

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FormatBytes formats bytes into human-readable format
func FormatBytes(bytes int64) string {
	const unit = 1024
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

// FormatBytesWithPrecision formats bytes with specified decimal precision
func FormatBytesWithPrecision(bytes int64, precision int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	format := fmt.Sprintf("%%.%df %%cB", precision)
	return fmt.Sprintf(format, float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatDuration formats duration into human-readable format.
//
// Formatting rules:
//   - Nanoseconds: whole number, no decimals (e.g., "123ns")
//   - Microseconds: whole number, no decimals (e.g., "456µs")
//   - Milliseconds: up to 3 decimal places (e.g., "123.456ms")
//   - Seconds: up to 2 decimal places (e.g., "45.67s")
//   - Minutes+: compound format (e.g., "3m 45.67s", "2h 30m 15s")
//   - Days+: compound format (e.g., "5d 12h 30m", "2y 3mo 15d")
func FormatDuration(d time.Duration) string {
	// Handle negative durations
	if d < 0 {
		return "-" + FormatDuration(-d)
	}

	// Handle zero
	if d == 0 {
		return "0s"
	}

	// Nanoseconds (< 1µs): whole number
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}

	// Microseconds (< 1ms): whole number
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}

	// Milliseconds (< 1s): up to 3 decimal places
	if d < time.Second {
		ms := float64(d.Nanoseconds()) / float64(time.Millisecond)
		return FormatFloat(ms, 3) + "ms"
	}

	// Seconds (< 1m): up to 2 decimal places
	if d < time.Minute {
		secs := float64(d.Nanoseconds()) / float64(time.Second)
		return FormatFloat(secs, 2) + "s"
	}

	// Minutes (< 1h): Xm + seconds with 2 decimal places
	if d < time.Hour {
		mins := int(d.Minutes())
		remainingSecs := float64(d-time.Duration(mins)*time.Minute) / float64(time.Second)
		if remainingSecs < 0.01 {
			return fmt.Sprintf("%dm", mins)
		}
		return fmt.Sprintf("%dm %ss", mins, FormatFloat(remainingSecs, 2))
	}

	// Hours (< 1 day): Xh Ym Zs (whole seconds)
	const day = 24 * time.Hour
	if d < day {
		hours := int(d.Hours())
		mins := int(d.Minutes()) % 60
		secs := int(d.Seconds()) % 60
		if secs == 0 && mins == 0 {
			return fmt.Sprintf("%dh", hours)
		}
		if secs == 0 {
			return fmt.Sprintf("%dh %dm", hours, mins)
		}
		return fmt.Sprintf("%dh %dm %ds", hours, mins, secs)
	}

	// Days (< 1 year): Xd Yh Zm (no seconds)
	const year = 365 * day
	const month = 30 * day // approximate
	if d < year {
		days := int(d / day)
		remaining := d % day
		hours := int(remaining.Hours())
		mins := int(remaining.Minutes()) % 60
		if hours == 0 && mins == 0 {
			return fmt.Sprintf("%dd", days)
		}
		if mins == 0 {
			return fmt.Sprintf("%dd %dh", days, hours)
		}
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}

	// Years: Xy Xmo Xd (approximate months as 30 days)
	years := int(d / year)
	remaining := d % year
	months := int(remaining / month)
	days := int((remaining % month) / day)
	if months == 0 && days == 0 {
		return fmt.Sprintf("%dy", years)
	}
	if days == 0 {
		return fmt.Sprintf("%dy %dmo", years, months)
	}
	return fmt.Sprintf("%dy %dmo %dd", years, months, days)
}

// FormatFloat formats a float with up to maxDecimals, trimming trailing zeros.
func FormatFloat(value float64, maxDecimals int) string {
	format := fmt.Sprintf("%%.%df", maxDecimals)
	s := fmt.Sprintf(format, value)

	// Trim trailing zeros after decimal point
	if strings.Contains(s, ".") {
		s = strings.TrimRight(s, "0")
		s = strings.TrimRight(s, ".")
	}
	return s
}

// FormatNumber formats a number with commas for readability
func FormatNumber(n int64) string {
	if n < 0 {
		return "-" + FormatNumber(-n)
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	s := fmt.Sprintf("%d", n)
	result := ""
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result += ","
		}
		result += string(c)
	}
	return result
}

// FormatPercent formats a percentage with specified precision
func FormatPercent(value float64, precision int) string {
	format := fmt.Sprintf("%%.%df%%%%", precision)
	return fmt.Sprintf(format, value)
}

// FormatRate formats a rate (items per second) with appropriate units
func FormatRate(count int64, duration time.Duration) string {
	if duration.Seconds() <= 0 {
		return "0/s"
	}
	rate := float64(count) / duration.Seconds()
	if rate >= 1000000 {
		return fmt.Sprintf("%.2fM/s", rate/1000000)
	}
	if rate >= 1000 {
		return fmt.Sprintf("%.2fK/s", rate/1000)
	}
	return fmt.Sprintf("%.2f/s", rate)
}

// WrapText wraps text to specified width
func WrapText(text string, width int) string {
	if len(text) <= width {
		return text
	}
	result := ""
	for i := 0; i < len(text); i += width {
		end := i + width
		if end > len(text) {
			end = len(text)
		}
		result += text[i:end] + "\n"
	}
	return result
}

// GetDirSize returns the total size of all files in a directory
func GetDirSize(path string) int64 {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size
}

// GetFileCount returns the number of files in a directory
func GetFileCount(path string) int {
	var count int
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count
}

// Uint32ToBytes converts a uint32 to a 4-byte big-endian slice
func Uint32ToBytes(n uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, n)
	return b
}

// BytesToUint32 converts a 4-byte big-endian slice to uint32
func BytesToUint32(b []byte) uint32 {
	if len(b) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

// Uint64ToBytes converts a uint64 to an 8-byte big-endian slice
func Uint64ToBytes(n uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, n)
	return b
}

// BytesToUint64 converts an 8-byte big-endian slice to uint64
func BytesToUint64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// HexStringToBytes converts a hex string to bytes
func HexStringToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	return hex.DecodeString(hexStr)
}

// BytesToHexString converts bytes to hex string
func BytesToHexString(b []byte) string {
	return hex.EncodeToString(b)
}

// BytesToGB converts a string representation of bytes to GB
func BytesToGB(bytesStr string) float64 {
	var bytes float64
	fmt.Sscanf(bytesStr, "%f", &bytes)
	return bytes / (1024 * 1024 * 1024)
}

// CalculateCompressionRatio calculates compression ratio as percentage reduction
func CalculateCompressionRatio(original, compressed int64) float64 {
	if original == 0 {
		return 0
	}
	return 100 * (1 - float64(compressed)/float64(original))
}

// CalculateOverhead calculates storage overhead as percentage
func CalculateOverhead(expected, actual int64) float64 {
	if expected == 0 {
		return 0
	}
	return float64(actual-expected) / float64(expected) * 100
}

// Min returns the minimum of two int64 values
func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// Max returns the maximum of two int64 values
func Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinUint32 returns the minimum of two uint32 values
func MinUint32(a, b uint32) uint32 {
	if a < b {
		return a
	}
	return b
}

// MaxUint32 returns the maximum of two uint32 values
func MaxUint32(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

// EnsureDir creates a directory if it doesn't exist
func EnsureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// FileExists checks if a file or directory exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// IsDir checks if path is a directory
func IsDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}
