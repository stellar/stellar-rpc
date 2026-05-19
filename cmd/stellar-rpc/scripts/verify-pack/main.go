// Throwaway verifier: opens a chunk .pack file, decodes one ledger,
// and prints its sequence + sha-of-bytes so we can confirm the round-trip.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	goxdr "github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/packfile"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/zstd"
)

func main() {
	path := flag.String("path", "", "pack file")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "--path required")
		os.Exit(2)
	}

	dec := zstd.NewDecompressor()
	r := packfile.Open(*path, packfile.ReaderOptions{RecordDecoder: dec})
	defer r.Close()

	tr, err := r.Trailer()
	must(err)
	total, err := r.TotalItems()
	must(err)
	hash, hasHash, err := r.ContentHash()
	must(err)
	fmt.Printf("trailer: format=%d records=%d totalItems=%d itemsPerRecord=%d indexSize=%d\n",
		tr.Format, tr.RecordCount, total, tr.ItemsPerRecord, tr.IndexSize)
	if hasHash {
		fmt.Printf("contentHash: %s\n", hex.EncodeToString(hash[:]))
	}

	check := func(pos int) {
		err := r.ReadItem(pos, func(b []byte) error {
			var lcm goxdr.LedgerCloseMeta
			if err := lcm.UnmarshalBinary(b); err != nil {
				return err
			}
			seq := lcm.LedgerSequence()
			sum := sha256.Sum256(b)
			fmt.Printf("pos %d: ledger seq=%d, %d bytes raw, sha256=%s\n",
				pos, seq, len(b), hex.EncodeToString(sum[:8]))
			return nil
		})
		must(err)
	}
	check(0)
	check(total / 2)
	check(total - 1)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
