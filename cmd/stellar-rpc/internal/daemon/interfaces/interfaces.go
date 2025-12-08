package interfaces

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	proto "github.com/stellar/go-stellar-sdk/protocols/stellarcore"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Daemon defines the interface that the Daemon would be implementing.
// this would be useful for decoupling purposes, allowing to test components without
// the actual daemon.
type Daemon interface {
	MetricsRegistry() *prometheus.Registry
	MetricsNamespace() string
	CoreClient() CoreClient
	FastCoreClient() FastCoreClient
	GetCore() *ledgerbackend.CaptiveStellarCore
}

type CoreClient interface {
	Info(ctx context.Context) (*proto.InfoResponse, error)
	SorobanInfo(ctx context.Context) (*proto.SorobanInfoResponse, error)
	SubmitTransaction(ctx context.Context, txBase64 string) (*proto.TXResponse, error)
}

type FastCoreClient interface {
	GetLedgerEntries(ctx context.Context, ledgerSeq uint32, keys ...xdr.LedgerKey) (proto.GetLedgerEntryResponse, error)
}
