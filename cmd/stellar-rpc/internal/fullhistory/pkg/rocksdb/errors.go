package rocksdb

import (
	"errors"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/fullhistory/pkg/stores"
)

// translateError maps wrapper closed-state sentinels to the
// stores-package equivalent. Other errors pass through unchanged.
// Used by every Layer-2 facade in this package.
func translateError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStoreClosed) {
		return stores.ErrStoreClosed
	}
	return err
}
