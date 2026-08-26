package catalog

import (
	"crypto/rand"
	"errors"
	"fmt"
)

// catalogSecretStoreKey holds the deployment's cold-index secret.
const catalogSecretStoreKey = "meta/catalog-secret"

// Secret returns a copy of the deployment's cold-index secret, minted once at
// Open and cached. Per-index secrets are derived from it, so an attacker who
// influences indexed keys cannot predict which block a key lands in. Returning
// a fixed-size array (not the internal slice) states the length and prevents a
// caller aliasing or mutating the cached value. Stable for the life of the
// catalog.
func (c *Catalog) Secret() [32]byte { return c.secret }

// ensureSecret loads the persisted cold-index secret, minting and persisting a
// fresh random one on first call. Open runs it single-threaded and caches the
// result; nothing else should call it (get-or-create is not atomic).
func (c *Catalog) ensureSecret() ([32]byte, error) {
	var s [32]byte
	v, found, err := c.get(catalogSecretStoreKey)
	if err != nil {
		return s, err
	}
	if found {
		if len(v) != len(s) {
			return s, fmt.Errorf("persisted cold-index secret is %d bytes, want %d", len(v), len(s))
		}
		copy(s[:], v)
		// An all-zero persisted secret is corruption or tampering, never a
		// mint (crypto/rand). HKDF would launder it into non-zero per-index
		// secrets that pass every downstream zero-check while making all
		// blinded routing attacker-predictable — the exact threat the secret
		// exists to close. Refuse to open.
		if s == ([32]byte{}) {
			return s, errors.New("persisted cold-index secret is all zero (corrupt store?)")
		}
		return s, nil
	}
	if _, err := rand.Read(s[:]); err != nil {
		return s, err
	}
	if err := c.put(catalogSecretStoreKey, string(s[:])); err != nil {
		return s, err
	}
	return s, nil
}
