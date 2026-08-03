package stores

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"

	"github.com/stellar/stellar-rpc/cmd/stellar-rpc/internal/rpcv2/packfile"
)

// PackReader is a packfile.Reader that has already applied the translation
// ErrCorrupt describes. Stores hold one of these rather than a raw
// *packfile.Reader, so the translation is a property of the handle instead of
// something each call site has to remember: there is no way to get an
// untranslated error out of a store's pack handle, and a method added here
// has to translate to compile.
//
// It forwards only what the stores use.
type PackReader struct{ r *packfile.Reader }

// OpenPack begins opening path in the background, exactly as packfile.Open
// does, and returns a handle whose errors are the stores sentinels.
func OpenPack(path string, opts packfile.ReaderOptions) *PackReader {
	return &PackReader{r: packfile.Open(path, opts)}
}

func (p *PackReader) Trailer() (packfile.Trailer, error) {
	t, err := p.r.Trailer()
	return t, translatePackErr(err)
}

func (p *PackReader) AppData() ([]byte, error) {
	b, err := p.r.AppData()
	return b, translatePackErr(err)
}

func (p *PackReader) TotalItems() (int, error) {
	n, err := p.r.TotalItems()
	return n, translatePackErr(err)
}

func (p *PackReader) ReadItem(position int, fn func([]byte) error) error {
	return translatePackErr(p.r.ReadItem(position, fn))
}

func (p *PackReader) ReadItems(
	ctx context.Context, positions []int, fn func(idx int, data []byte) error,
) error {
	return translatePackErr(p.r.ReadItems(ctx, positions, fn))
}

// ReadRange translates per yielded element, since a range read reports failure
// on the element that failed rather than from the call. Measured at roughly 3ns
// an element against a real scan's ~470ns of decode.
func (p *PackReader) ReadRange(start, count int) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		for data, err := range p.r.ReadRange(start, count) {
			if !yield(data, translatePackErr(err)) {
				return
			}
		}
	}
}

// Close reports the deferred open error as well as the close itself, so it is
// the first place an open-time failure surfaces on a handle that was never
// read — and it owes callers the same sentinel as every other method.
func (p *PackReader) Close() error { return translatePackErr(p.r.Close()) }

// translatePackErr maps packfile- and os-level errors to the sentinels above.
// Corruption is wrapped rather than replaced, so a caller can still reach the
// specific cause (ErrMagic, ErrChecksum, ...) underneath ErrCorrupt.
func translatePackErr(err error) error {
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
