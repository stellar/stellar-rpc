//nolint:lll
package xdr2json

/*
// See preflight.go for add'l explanations:
// Note: no blank lines allowed.
#include <stdlib.h>
#include "../../lib/xdr2json.h"
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-pc-windows-gnu/release-with-panic-unwind/ -lxdr2json -lntdll -static -lws2_32 -lbcrypt -luserenv
#cgo darwin,amd64  LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-apple-darwin/release-with-panic-unwind/ -lxdr2json -ldl -lm
#cgo darwin,arm64  LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-apple-darwin/release-with-panic-unwind/ -lxdr2json -ldl -lm
#cgo linux,amd64   LDFLAGS: -L${SRCDIR}/../../../../target/x86_64-unknown-linux-gnu/release-with-panic-unwind/ -lxdr2json -ldl -lm
#cgo linux,arm64   LDFLAGS: -L${SRCDIR}/../../../../target/aarch64-unknown-linux-gnu/release-with-panic-unwind/ -lxdr2json -ldl -lm
*/
import "C"

import (
	"encoding"
	"encoding/json"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

// ConvertBytes takes an XDR object (`xdr`) and its serialized bytes (`field`)
// and returns the raw JSON-formatted serialization of that object.
// It can be unmarshalled to a proper JSON structure, but the raw bytes are
// returned to avoid unnecessary round-trips. If there is an
// error, it returns an empty string.
//
// The `xdr` object does not need to actually be initialized/valid:
// we only use it to determine the name of the structure. We could just
// accept a string, but that would make mistakes likelier than passing the
// structure itself (by reference).
func ConvertBytes(xdr interface{}, field []byte) (json.RawMessage, error) {
	if len(field) == 0 {
		return []byte(""), nil
	}

	xdrTypeName := reflect.TypeOf(xdr).Name()
	return convertAnyBytes(xdrTypeName, field)
}

// ConvertInterface takes a valid XDR object (`xdr`) and returns
// the raw JSON-formatted serialization of that object. If there is an
// error, it returns an empty string.
//
// Unlike `ConvertBytes`, the value here needs to be valid and
// serializable.
func ConvertInterface(xdr encoding.BinaryMarshaler) (json.RawMessage, error) {
	xdrTypeName := reflect.TypeOf(xdr).Name()
	data, err := xdr.MarshalBinary()
	if err != nil {
		return []byte(""), errors.Wrapf(err, "failed to serialize XDR type '%s'", xdrTypeName)
	}

	return convertAnyBytes(xdrTypeName, data)
}

func convertAnyBytes(xdrTypeName string, field []byte) (json.RawMessage, error) {
	var jsonStr, errStr string
	// scope just added to show matching alloc/frees
	{
		goRawXdr := CXDR(field)
		defer FreeGoXDR(goRawXdr)

		b := C.CString(xdrTypeName)
		defer C.free(unsafe.Pointer(b))

		result := C.xdr_to_json(b, goRawXdr)
		defer C.free_conversion_result(result)

		jsonStr = C.GoString(result.json)
		errStr = C.GoString(result.error)
	}

	if errStr != "" {
		return json.RawMessage(jsonStr), errors.New(errStr)
	}

	return json.RawMessage(jsonStr), nil
}

// CXDR is ripped directly from preflight.go to avoid a dependency.
func CXDR(xdr []byte) C.xdr_t {
	return C.xdr_t{
		xdr: (*C.uchar)(C.CBytes(xdr)),
		len: C.size_t(len(xdr)),
	}
}

func FreeGoXDR(xdr C.xdr_t) {
	C.free(unsafe.Pointer(xdr.xdr))
}
