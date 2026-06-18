// Bundle tooling for the monthly load-test corpus refresh (see README.md and
// refresh-bundles.sh alongside). Modes:
//
//	(default) FILE...         per-bundle ledger count, seq monotonicity, and a
//	                          histogram of per-ledger tx/op-type profiles
//	-concat FILE...           additionally verify the byte-concatenation of all
//	                          inputs reads as one stream (the CI ingest shape)
//	-dupes PASSPHRASE FILE... intra- and cross-bundle tx hash duplicate check;
//	                          exits 3 on duplicates (these abort CI ingestion
//	                          on the transactions.hash unique index)
//	-duploc PASSPHRASE A B    locate which ledgers hold the duplicated hashes
//	-trim N -out F FILE       first N ledgers of a .xdr.zstd bundle -> F
//	                          (for local trimmed e2e fixtures)
//	-trimlast N FILE          last N frames of a RAW record-marked meta.xdr
//	                          (stellar-core METADATA_OUTPUT_STREAM) verbatim
//	                          to stdout; pipe through `zstd -19` for a bundle
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/klauspost/compress/zstd"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const lastFragment = 0x80000000

// trimLastRaw streams the last keep frames of a record-marked XDR stream
// (4-byte big-endian header, high bit = last-fragment flag, low 31 bits =
// length) verbatim to w. apply-load emits its benchmark ledgers last, so this
// is how a raw meta.xdr becomes a corpus bundle.
func trimLastRaw(path string, keep int, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Pass 1: record every frame's byte offset.
	var offsets []int64
	var pos int64
	hdr := make([]byte, 4)
	for {
		if _, err := io.ReadFull(f, hdr); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("reading frame header at offset %d: %w", pos, err)
		}
		n := binary.BigEndian.Uint32(hdr)
		if n&lastFragment == 0 {
			return fmt.Errorf("fragmented record at offset %d is unsupported", pos)
		}
		offsets = append(offsets, pos)
		pos += 4 + int64(n&^lastFragment)
		if _, err := f.Seek(pos, io.SeekStart); err != nil {
			return err
		}
	}
	if keep > len(offsets) {
		return fmt.Errorf("stream has only %d frames, wanted %d", len(offsets), keep)
	}

	// Pass 2: frames are contiguous through EOF, so one copy starting at the
	// (len-keep)'th offset emits exactly the last keep frames.
	if _, err := f.Seek(offsets[len(offsets)-keep], io.SeekStart); err != nil {
		return err
	}
	_, err = io.Copy(w, f)
	return err
}

// txHashes returns the set of transaction hashes in the bundle, computed
// under the given network passphrase (the same way RPC ingestion keys the
// transactions table).
func txHashes(path, passphrase string) (map[xdr.Hash]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stream, err := xdr.NewZstdStream(f)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	hashes := make(map[xdr.Hash]struct{})
	for {
		var ledger xdr.LedgerCloseMeta
		if err := stream.ReadOne(&ledger); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		for _, env := range ledger.TransactionEnvelopes() {
			h, err := network.HashTransactionInEnvelope(env, passphrase)
			if err != nil {
				return nil, err
			}
			hashes[h] = struct{}{}
		}
	}
	return hashes, nil
}

// txHashLedgers maps every tx hash in the bundle to the ledger seqs containing it.
func txHashLedgers(path, passphrase string) (map[xdr.Hash][]uint32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stream, err := xdr.NewZstdStream(f)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	out := make(map[xdr.Hash][]uint32)
	for {
		var ledger xdr.LedgerCloseMeta
		if err := stream.ReadOne(&ledger); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		seq := ledger.LedgerSequence()
		for _, env := range ledger.TransactionEnvelopes() {
			h, err := network.HashTransactionInEnvelope(env, passphrase)
			if err != nil {
				return nil, err
			}
			out[h] = append(out[h], seq)
		}
	}
	return out, nil
}

// reportDupeLocations prints, for the overlap between two bundles, a histogram
// of which ledgers (in each bundle) hold the duplicated txs.
func reportDupeLocations(pathA, pathB, passphrase string) error {
	a, err := txHashLedgers(pathA, passphrase)
	if err != nil {
		return err
	}
	b, err := txHashLedgers(pathB, passphrase)
	if err != nil {
		return err
	}
	ledgersA := map[uint32]int{}
	ledgersB := map[uint32]int{}
	total := 0
	for h, seqsA := range a {
		if seqsB, ok := b[h]; ok {
			total++
			for _, s := range seqsA {
				ledgersA[s]++
			}
			for _, s := range seqsB {
				ledgersB[s]++
			}
		}
	}
	fmt.Printf("dup hashes: %d\n", total)
	printHist := func(name string, m map[uint32]int) {
		keys := make([]int, 0, len(m))
		for k := range m {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		fmt.Printf("%s: %d ledgers contain dups; ledgerSeq:count =", name, len(m))
		for i, k := range keys {
			if i >= 40 {
				fmt.Printf(" ...(%d more)", len(keys)-40)
				break
			}
			fmt.Printf(" %d:%d", k, m[uint32(k)])
		}
		fmt.Println()
	}
	printHist(pathA, ledgersA)
	printHist(pathB, ledgersB)
	return nil
}

// reportDupes prints intra-bundle and pairwise cross-bundle tx hash overlap.
// Returns true if any duplicates were found.
func reportDupes(paths []string, passphrase string) (bool, error) {
	sets := make([]map[xdr.Hash]struct{}, len(paths))
	counts := make([]int, len(paths))
	for i, p := range paths {
		st, err := inspect(p)
		if err != nil {
			return false, err
		}
		counts[i] = st.totalTxs
		if sets[i], err = txHashes(p, passphrase); err != nil {
			return false, err
		}
		fmt.Printf("%s: %d txs, %d distinct hashes\n", p, counts[i], len(sets[i]))
	}
	dupes := false
	for i := range sets {
		if len(sets[i]) != counts[i] {
			fmt.Printf("INTRA-BUNDLE DUPES in %s: %d\n", paths[i], counts[i]-len(sets[i]))
			dupes = true
		}
		for j := i + 1; j < len(sets); j++ {
			overlap := 0
			for h := range sets[i] {
				if _, ok := sets[j][h]; ok {
					overlap++
				}
			}
			fmt.Printf("overlap %s x %s: %d\n", paths[i], paths[j], overlap)
			if overlap > 0 {
				dupes = true
			}
		}
	}
	return dupes, nil
}

// trim writes the first n ledgers of the bundle at src to dst as a framed,
// zstd-compressed stream (the same format the bundles use).
func trim(src, dst string, n int) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	stream, err := xdr.NewZstdStream(f)
	if err != nil {
		return err
	}
	defer stream.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	w, err := zstd.NewWriter(out)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		var ledger xdr.LedgerCloseMeta
		if err := stream.ReadOne(&ledger); err == io.EOF {
			return fmt.Errorf("bundle has only %d ledgers, wanted %d", i, n)
		} else if err != nil {
			return err
		}
		if err := xdr.MarshalFramed(w, ledger); err != nil {
			return err
		}
	}
	if err := w.Close(); err != nil {
		return err
	}
	return out.Close()
}

type profile struct {
	txs, classic, soroban, otherOps int
}

type bundleStats struct {
	ledgers           int
	firstSeq, lastSeq uint32
	monotonic         bool
	totalTxs          int
	totalClassic      int
	totalSoroban      int
	totalOtherOps     int
	profiles          map[profile]int
}

func inspect(path string) (bundleStats, error) {
	f, err := os.Open(path)
	if err != nil {
		return bundleStats{}, err
	}
	defer f.Close()
	stream, err := xdr.NewZstdStream(f)
	if err != nil {
		return bundleStats{}, err
	}
	defer stream.Close()

	st := bundleStats{monotonic: true, profiles: map[profile]int{}}
	var prevSeq uint32
	for {
		var ledger xdr.LedgerCloseMeta
		if err := stream.ReadOne(&ledger); err == io.EOF {
			break
		} else if err != nil {
			return st, fmt.Errorf("ledger %d: %w", st.ledgers+1, err)
		}
		seq := ledger.LedgerSequence()
		if st.ledgers == 0 {
			st.firstSeq = seq
		} else if seq != prevSeq+1 {
			st.monotonic = false
		}
		prevSeq = seq
		st.lastSeq = seq

		var p profile
		for _, env := range ledger.TransactionEnvelopes() {
			p.txs++
			for _, op := range env.Operations() {
				switch op.Body.Type {
				case xdr.OperationTypePayment:
					p.classic++
				case xdr.OperationTypeInvokeHostFunction:
					p.soroban++
				default:
					p.otherOps++
				}
			}
		}
		st.profiles[p]++
		st.totalTxs += p.txs
		st.totalClassic += p.classic
		st.totalSoroban += p.soroban
		st.totalOtherOps += p.otherOps
		st.ledgers++
	}
	return st, nil
}

func report(name string, st bundleStats) {
	fmt.Printf("=== %s\n", name)
	fmt.Printf("ledgers=%d seq=[%d..%d] monotonic=%v\n", st.ledgers, st.firstSeq, st.lastSeq, st.monotonic)
	fmt.Printf("totals: txs=%d classicPaymentOps=%d sorobanInvokeOps=%d otherOps=%d\n",
		st.totalTxs, st.totalClassic, st.totalSoroban, st.totalOtherOps)
	keys := make([]profile, 0, len(st.profiles))
	for k := range st.profiles {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return st.profiles[keys[i]] > st.profiles[keys[j]] })
	for _, k := range keys {
		fmt.Printf("  %6d ledgers with txs=%d classic=%d soroban=%d other=%d\n",
			st.profiles[k], k.txs, k.classic, k.soroban, k.otherOps)
	}
}

func main() {
	concat := flag.Bool("concat", false, "also verify byte-concatenation of all inputs reads as one stream")
	trimN := flag.Int("trim", 0, "write the first N ledgers of the (single) input to -out and exit")
	trimOut := flag.String("out", "", "output path for -trim")
	trimLastN := flag.Int("trimlast", 0, "emit the last N frames of a raw meta.xdr to stdout and exit")
	dupes := flag.String("dupes", "", "network passphrase; check intra+cross-bundle tx hash duplicates and exit")
	duploc := flag.String("duploc", "", "network passphrase; locate cross-bundle dup hashes between exactly two inputs and exit")
	flag.Parse()
	paths := flag.Args()
	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "usage: refresh-tool [-concat|-dupes P|-duploc P|-trim N -out FILE|-trimlast N] FILE...")
		os.Exit(2)
	}

	if *trimLastN > 0 {
		if len(paths) != 1 {
			fmt.Fprintln(os.Stderr, "-trimlast needs exactly one input")
			os.Exit(2)
		}
		if err := trimLastRaw(paths[0], *trimLastN, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		return
	}

	if *duploc != "" {
		if len(paths) != 2 {
			fmt.Fprintln(os.Stderr, "-duploc needs exactly two inputs")
			os.Exit(2)
		}
		if err := reportDupeLocations(paths[0], paths[1], *duploc); err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		return
	}

	if *dupes != "" {
		found, err := reportDupes(paths, *dupes)
		if err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		if found {
			fmt.Println("DUPLICATES FOUND")
			os.Exit(3)
		}
		fmt.Println("no duplicate tx hashes")
		return
	}

	if *trimN > 0 {
		if len(paths) != 1 || *trimOut == "" {
			fmt.Fprintln(os.Stderr, "-trim needs exactly one input and -out")
			os.Exit(2)
		}
		if err := trim(paths[0], *trimOut, *trimN); err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		fmt.Printf("wrote first %d ledgers of %s to %s\n", *trimN, paths[0], *trimOut)
		return
	}

	sumLedgers, sumTxs := 0, 0
	for _, p := range paths {
		st, err := inspect(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL %s: %v\n", p, err)
			os.Exit(1)
		}
		report(p, st)
		sumLedgers += st.ledgers
		sumTxs += st.totalTxs
	}

	if *concat && len(paths) > 1 {
		tmp, err := os.CreateTemp("", "concat-*.xdr.zstd")
		if err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		defer os.Remove(tmp.Name())
		for _, p := range paths {
			f, err := os.Open(p)
			if err != nil {
				fmt.Fprintln(os.Stderr, "FATAL:", err)
				os.Exit(1)
			}
			if _, err := io.Copy(tmp, f); err != nil {
				fmt.Fprintln(os.Stderr, "FATAL:", err)
				os.Exit(1)
			}
			f.Close()
		}
		if err := tmp.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "FATAL:", err)
			os.Exit(1)
		}
		st, err := inspect(tmp.Name())
		if err != nil {
			fmt.Fprintf(os.Stderr, "FATAL concat read: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("=== CONCATENATED (%d files)\n", len(paths))
		fmt.Printf("ledgers=%d (sum of parts=%d) txs=%d (sum of parts=%d)\n",
			st.ledgers, sumLedgers, st.totalTxs, sumTxs)
		if st.ledgers != sumLedgers || st.totalTxs != sumTxs {
			fmt.Fprintln(os.Stderr, "FATAL: concatenated stream does not equal sum of parts")
			os.Exit(1)
		}
		fmt.Println("concat-as-one: OK")
	}
}
