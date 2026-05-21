//go:build linux

package main

import "syscall"

// platformDirectFlag is the open(2) flag that bypasses the page
// cache on this platform. On Linux that's O_DIRECT; reads must use a
// block-aligned buffer at a block-aligned offset, which the
// fileReader in streamhash_merge.go already guarantees.
const platformDirectFlag = syscall.O_DIRECT
