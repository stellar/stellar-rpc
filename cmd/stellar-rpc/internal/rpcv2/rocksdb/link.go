package rocksdb

// librocksdb is a static archive, and on Linux a static archive resolves only
// against the libraries that come after it on the link line. grocksdb is built
// with the grocksdb_clean_link tag, so its own line is
// "-lrocksdb -pthread -lstdc++ -ldl" and names neither zstd nor libm. Go emits
// each package's LDFLAGS in package load order, so the "-lzstd" that the zstd
// package adds can land before "-lrocksdb". This line keeps everything
// librocksdb.a needs after it, whatever the package order. A library that is
// named twice is searched twice, which is harmless.

/*
#cgo linux LDFLAGS: -lrocksdb -lzstd -lstdc++ -lm
*/
import "C"
