package methods

import (
	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/handler"
)

func NewHandler(fn any) jrpc2.Handler {
	fi, err := handler.Check(fn)
	if err != nil {
		panic(err)
	}
	// explicitly disable array arguments since otherwise we cannot add
	// new method arguments without breaking backwards compatibility with clients
	fi.AllowArray(false)
	return fi.Wrap()
}
