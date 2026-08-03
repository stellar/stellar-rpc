package stores

import (
	"errors"
	"fmt"
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// TranslatePackErr maps a packfile- or os-level error to the sentinels above.
// Every store reading a packfile applies it at its public-method boundaries;
// it lives here rather than in each store so the two cannot drift.
//
// Corruption is wrapped rather than replaced, so a caller can still reach the
// specific cause (ErrMagic, ErrChecksum, ...) underneath ErrCorrupt.
func TranslatePackErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, os.ErrClosed):
		return ErrStoreClosed
	case errors.Is(err, packfile.ErrCorrupt):
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	default:
		return err
	}
}
