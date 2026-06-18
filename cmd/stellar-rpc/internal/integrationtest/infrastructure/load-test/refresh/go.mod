// Standalone module so the repo's `go build ./...` ignores this tooling.
module refresh-tools

go 1.25

require (
	github.com/klauspost/compress v1.18.0
	github.com/stellar/go-stellar-sdk v0.5.1-0.20260506215501-d170348eff6c
)

require (
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stellar/go-xdr v0.0.0-20260423131911-a87d4d0789c3 // indirect
)
