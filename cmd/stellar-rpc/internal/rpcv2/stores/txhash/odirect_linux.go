package txhash

import "syscall"

// directOpenFlag returns O_DIRECT on Linux so the cold-index merge reads
// .bin files straight into our aligned buffers, bypassing the page
// cache. The build streams the whole input once and never re-reads it,
// so caching those bytes only evicts pages something else actually
// needs. newFileReader falls back to a cached open if a filesystem
// rejects O_DIRECT (e.g. tmpfs).
func directOpenFlag() int { return syscall.O_DIRECT }
