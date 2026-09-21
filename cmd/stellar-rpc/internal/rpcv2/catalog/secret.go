package catalog

import (
	"crypto/rand"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/chunk"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/event"
	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/stores/txhash"
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

// TxHashIndexSecret derives the routing secret of the tx-hash index that
// covers chunk c: the one the chunk's .bin keys are blinded with and the
// index build adopts.
func (c *Catalog) TxHashIndexSecret(ch chunk.ID) [stores.SecretLen]byte {
	return txhash.ColdIndexSecret(c.secret[:], uint32(c.txhashIndex.TxHashIndexID(ch)))
}

// EventsIndexSecret derives the routing secret of chunk c's events cold index.
func (c *Catalog) EventsIndexSecret(ch chunk.ID) [stores.SecretLen]byte {
	return event.ColdIndexSecret(c.secret[:], ch)
}

// ensureSecret loads the persisted cold-index secret, minting and persisting a
// fresh random one on first call. Open runs it single-threaded, after the
// census has already validated any persisted value's width, and caches the
// result; nothing else should call it (get-or-create is not atomic).
func (c *Catalog) ensureSecret() ([32]byte, error) {
	s, found, err := c.loadSecret()
	if err != nil || found {
		return s, err
	}
	if _, err := rand.Read(s[:]); err != nil {
		return s, err
	}
	if err := c.put(catalogSecretStoreKey, string(s[:])); err != nil {
		return s, err
	}
	return s, nil
}

// loadSecret reads the persisted cold-index secret; found is false on a
// catalog that has never minted one.
func (c *Catalog) loadSecret() ([32]byte, bool, error) {
	var s [32]byte
	v, found, err := c.get(catalogSecretStoreKey)
	if err != nil || !found {
		return s, false, err
	}
	copy(s[:], v)
	return s, true, nil
}
