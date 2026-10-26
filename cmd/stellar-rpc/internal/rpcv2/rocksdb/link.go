package rocksdb

// On Linux a static archive resolves only against libraries listed after it,
// and Go emits each package's LDFLAGS in package load order, so the zstd
// package's "-lzstd" can land before grocksdb's "-lrocksdb". This line puts
// everything librocksdb.a needs after it. A library named twice is harmless.

/*
#cgo linux LDFLAGS: -lrocksdb -lzstd -lstdc++ -lm
*/
import "C"
