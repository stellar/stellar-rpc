package stores

// Keyed block routing of cold streamhash indexes.
//
// streamhash's block routing is public and unkeyed, so an attacker could
// grind keys into one block and abort the build (ErrBlockOverflow).
// Feeding BlindKey(secret, key) at both build and query makes block
// placement unpredictable without the per-index secret.

import (
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/dchest/siphash"
	"golang.org/x/crypto/hkdf"
)

// SecretLen is the byte length of a routing secret (a SipHash key).
const SecretLen = 16

// DeriveIndexSecret derives one index's secret from the deployment's catalog
// secret: HKDF-SHA256(catalogSecret, info = domain ‖ indexID big-endian).
// Deterministic, so the same secret is derivable at ingest, build, and query
// time and rebuilds stay byte-identical.
func DeriveIndexSecret(catalogSecret []byte, domain string, indexID uint32) [SecretLen]byte {
	if len(catalogSecret) == 0 {
		panic("stores: DeriveIndexSecret requires a non-empty catalog secret")
	}
	info := binary.BigEndian.AppendUint32([]byte(domain), indexID)
	var s [SecretLen]byte
	if _, err := io.ReadFull(hkdf.New(sha256.New, catalogSecret, nil, info), s[:]); err != nil {
		panic(err) // unreachable: SecretLen is far below HKDF-SHA256's output limit
	}
	return s
}

// BlindKey returns SipHash-2-4-128(secret, key), secret split and output
// packed little-endian (the SipHash reference byte layout).
func BlindKey(secret [SecretLen]byte, key []byte) [SecretLen]byte {
	r0, r1 := siphash.Hash128(
		binary.LittleEndian.Uint64(secret[0:8]),
		binary.LittleEndian.Uint64(secret[8:16]),
		key,
	)
	var out [SecretLen]byte
	binary.LittleEndian.PutUint64(out[0:8], r0)
	binary.LittleEndian.PutUint64(out[8:16], r1)
	return out
}
