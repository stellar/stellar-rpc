package db

import (
	"github.com/stellar/go/xdr"
)

type LedgerKeyAndEntry struct {
	Key                xdr.LedgerKey
	Entry              xdr.LedgerEntry
	LiveUntilLedgerSeq *uint32 // optional live-until ledger seq, when applicable.
}
