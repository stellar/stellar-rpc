package catalog

import (
	"crypto/rand"
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
// fresh random one on first call. Open runs it single-threaded, after the
// census has already validated any persisted value's width, and caches the
// result; nothing else should call it (get-or-create is not atomic).
func (c *Catalog) ensureSecret() ([32]byte, error) {
	var s [32]byte
	v, found, err := c.get(catalogSecretStoreKey)
	if err != nil {
		return s, err
	}
	if found {
		copy(s[:], v)
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
