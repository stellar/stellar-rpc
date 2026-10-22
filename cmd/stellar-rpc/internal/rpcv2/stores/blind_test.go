package stores

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

// refSecret is the SipHash reference test key: bytes 00..0f.
func refSecret() [SecretLen]byte {
	var s [SecretLen]byte
	for i := range s {
		s[i] = byte(i)
	}
	return s
}

// TestBlindKey_KnownAnswer pins BlindKey to the SipHash-2-4 128-bit
// reference vectors — any change to primitive, key split, or output
// packing breaks every existing keyed index.
func TestBlindKey_KnownAnswer(t *testing.T) {
	msg := make([]byte, 16)
	for i := range msg {
		msg[i] = byte(i)
	}
	for _, tc := range []struct {
		msgLen  int
		wantHex string
	}{
		{8, "3b62a9ba6258f5610f83e264f31497b4"},
		{16, "6ee2a4ca67b054bbfd3315bf85230577"},
	} {
		got := BlindKey(refSecret(), msg[:tc.msgLen])
		assert.Equal(t, tc.wantHex, hex.EncodeToString(got[:]), "msg len %d", tc.msgLen)
	}
}

func TestBlindKey_Deterministic(t *testing.T) {
	secret := refSecret()
	key := []byte("0123456789abcdef")

	first := BlindKey(secret, key)
	for range 5 {
		assert.Equal(t, first, BlindKey(secret, key))
	}

	other := refSecret()
	other[0] ^= 0xff
	assert.NotEqual(t, first, BlindKey(other, key))
	assert.NotEqual(t, first, BlindKey(secret, []byte("0123456789abcdeg")))
}

// TestDeriveIndexSecret_KnownAnswer pins DeriveIndexSecret to fixed
// HKDF-SHA256(catalogSecret, nil, domain‖indexID(BE)) vectors — any change to the
// KDF, info layout, or output length breaks every existing keyed index.
func TestDeriveIndexSecret_KnownAnswer(t *testing.T) {
	catalogSecret := bytes.Repeat([]byte{0x0b}, 32)
	for _, tc := range []struct {
		indexID uint32
		wantHex string
	}{
		{0, "16a4530128a224e453bdc45e21190366"},
		{1, "7e84f0acec98ee0c91262f4dbcbc84f1"},
	} {
		got := DeriveIndexSecret(catalogSecret, "txhash", tc.indexID)
		assert.Equal(t, tc.wantHex, hex.EncodeToString(got[:]), "index %d", tc.indexID)
	}
}

func TestDeriveIndexSecret_DeterministicAndDomainSeparated(t *testing.T) {
	catalogSecret := bytes.Repeat([]byte{0x42}, 32)

	first := DeriveIndexSecret(catalogSecret, "txhash", 3)
	assert.Equal(t, first, DeriveIndexSecret(catalogSecret, "txhash", 3), "same inputs, same secret")

	assert.NotEqual(t, first, DeriveIndexSecret(catalogSecret, "txhash", 4), "index ID separates")
	assert.NotEqual(t, first, DeriveIndexSecret(catalogSecret, "events", 3), "domain separates")
	assert.NotEqual(t, first, DeriveIndexSecret(bytes.Repeat([]byte{0x43}, 32), "txhash", 3), "master key separates")
}
