package protocol

import (
	"fmt"
	"strings"
)

const (
	FormatBase64 = "base64"
	FormatJSON   = "json"
)

var errInvalidFormat = fmt.Errorf(
	"expected %s for optional 'xdrFormat'",
	strings.Join([]string{FormatBase64, FormatJSON}, ", "))

func IsValidFormat(format string) error {
	switch format {
	case "":
	case FormatJSON:
	case FormatBase64:
	default:
		return fmt.Errorf("got '%s': %w", format, errInvalidFormat)
	}
	return nil
}
