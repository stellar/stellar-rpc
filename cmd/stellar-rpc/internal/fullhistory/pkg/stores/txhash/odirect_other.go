//go:build !linux

package txhash

// directOpenFlag is 0 on non-Linux platforms, which either lack
// O_DIRECT or expose cache-bypass differently (e.g. darwin's F_NOCACHE
// fcntl). The aligned-ReadAt path in newFileReader is correct either
// way — only the page-cache bypass is skipped.
func directOpenFlag() int { return 0 }
