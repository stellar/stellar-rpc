package runset

// secret.go — the routing-secret half of the protocol the two hot engines
// share. Like the publish protocol above it, this was one rule written
// twice: byte-identical adopt/require pairs in stores/txhash and
// stores/event that differed only in a store key and an error prefix.
//
// Unlike the manifest twins (deliberate — two engines, two keys, nothing to
// version in lockstep), this pair encodes a CROSS-engine invariant:
// hotchunk.Secrets hands both engines their secret from one derivation, and
// a chunk is only correctly keyed if BOTH adopt and require it by the same
// rule. Two copies of that rule is a thing to keep in agreement, so it lives
// here once.

import (
	"bytes"
	"errors"
	"fmt"
)

// KV is the least of a chunk store this protocol needs: one namespaced get
// and one DURABLE put. Deliberately an interface over two methods rather
// than a *rocksdb.Store, so runset stays import-free of the storage layer
// (and a fake KV can drive the table test). Both engines satisfy it with
// their shared *rocksdb.Store; the durability of Put is the store's job
// (rocksdb.Store.Put rides a synced write, which is what makes adoption
// durable before any run keyed with it can exist).
type KV interface {
	Get(cf string, key []byte) ([]byte, bool, error)
	Put(cf string, key, value []byte) error
}

// secretCF is the column family the adopted secret lives in: the default
// one, beside each engine's run manifest. The KEY is the engine's (one per
// store), the CF is shared.
const secretCF = ""

// AdoptSecret pins secret as THE routing secret of this chunk DB's engine
// state: the first open persists it, every later open must present the same
// one. Sealed runs are blinded with it and the chunk's cold artifact
// inherits their bytes (txhash copies run records into the .bin verbatim;
// events merges run keys into the index), so a chunk whose runs were written
// under one secret and frozen under another would produce an artifact no
// query could route — a silent, undetectable-at-read wrong answer. The
// mismatch is loud instead.
//
// engine prefixes the errors ("txhash", "events"); key is the engine's
// secret key. There is no migration and no re-keying: a re-minted catalog
// secret means the chunk must be re-ingested (pre-release posture — hot
// chunks are bound to the catalog secret by design, while cold artifacts
// self-describe and survive it).
func AdoptSecret(kv KV, key []byte, engine string, secret []byte) error {
	if allZero(secret) {
		return fmt.Errorf("%s: all-zero routing secret (blinding disabled — derive a per-index secret)", engine)
	}
	stored, found, err := kv.Get(secretCF, key)
	if err != nil {
		return fmt.Errorf("%s: read routing secret: %w", engine, err)
	}
	if !found {
		if perr := kv.Put(secretCF, key, secret); perr != nil {
			return fmt.Errorf("%s: persist routing secret: %w", engine, perr)
		}
		return nil
	}
	if !bytes.Equal(stored, secret) {
		return fmt.Errorf("%s: hot DB is keyed under a different routing secret "+
			"(catalog remint? no migration — re-ingest the chunk)", engine)
	}
	return nil
}

// RequireSecret is the read-ONLY half of the same rule, for the freeze: it
// may not adopt (the store is read-only, and nothing about a freeze should
// mint durable state), only insist that the secret it was handed is the one
// the DB's sealed runs are keyed with. Errors carry no engine prefix — every
// caller already wraps them with its own engine and chunk.
//
// A DB with no persisted secret is ACCEPTED, and the reason that is safe is
// the run-format magic, not the absence itself. A pre-release build of
// either engine sealed RAW-keyed runs into a DB it never stamped a secret
// onto; nothing about those runs' bytes distinguishes them from this build's
// blinded ones, so this check cannot tell them apart and does not try. What
// stops them is that such a run declares an older magic — txhash TXHRUN01,
// events EVR2 — and dies at open, in the freeze's own run opener, before a
// byte of artifact is written. So the only DBs this acceptance lets through
// are one whose runs this build wrote (magic current, keys blinded, secret
// stamped, and then the equality check below applies) and one with no runs
// at all, which is all raw tail the freeze blinds itself with exactly this
// secret.
func RequireSecret(kv KV, key []byte, secret []byte) error {
	stored, found, err := kv.Get(secretCF, key)
	if err != nil {
		return fmt.Errorf("read routing secret: %w", err)
	}
	if found && !bytes.Equal(stored, secret) {
		return errors.New("hot DB is keyed under a different routing secret than this freeze " +
			"(catalog remint? no migration — re-ingest the chunk)")
	}
	return nil
}

// allZero reports whether secret carries no key material at all — the
// "blinding effectively disabled" shape, refused outright so a zero-valued
// Secrets struct can never key a chunk. An empty slice counts as zero.
func allZero(secret []byte) bool {
	for _, b := range secret {
		if b != 0 {
			return false
		}
	}
	return true
}
