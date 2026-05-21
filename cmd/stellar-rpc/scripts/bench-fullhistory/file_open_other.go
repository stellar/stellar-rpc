//go:build !linux

package main

// platformDirectFlag is 0 (no-op) on non-Linux platforms. The
// fileReader's aligned-ReadAt path still runs and produces correct
// results; reads just go through the page cache like any other I/O.
// macOS could expose F_NOCACHE via a separate fcntl call, but the
// bench is Linux-only in practice so we don't bother.
const platformDirectFlag = 0
