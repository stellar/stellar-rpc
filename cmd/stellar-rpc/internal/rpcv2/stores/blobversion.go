package stores

import (
	"errors"
	"fmt"
)

// CheckBlobVersion gates a blob that leads with its own version byte: an
// empty blob and an unrecognized version each refuse, the version checked
// before any length so a differently-sized newer-format blob reports as an
// upgrade problem rather than a corruption-shaped size mismatch. Callers
// keep their own length checks on the remainder.
func CheckBlobVersion(blob []byte, want byte) error {
	if len(blob) == 0 {
		return errors.New("empty blob")
	}
	if blob[0] != want {
		return fmt.Errorf("unsupported version 0x%02x, want 0x%02x (written by a newer stellar-rpc?)",
			blob[0], want)
	}
	return nil
}
