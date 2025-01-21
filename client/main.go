package client

import (
	"context"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/jhttp"

	"github.com/stellar/stellar-rpc/protocol"
)

type Client struct {
	url  string
	cli  *jrpc2.Client
	opts *jrpc2.ClientOptions
}

func NewClient(url string, opts *jrpc2.ClientOptions) *Client {
	c := &Client{url: url, opts: opts}
	c.refreshClient()
	return c
}

func (c *Client) refreshClient() {
	if c.cli != nil {
		c.cli.Close()
	}
	ch := jhttp.NewChannel(c.url, nil)
	c.cli = jrpc2.NewClient(ch, c.opts)
}

func (c *Client) callResult(ctx context.Context, method string, params, result any) error {
	err := c.cli.CallResult(ctx, method, params, result)
	if err != nil {
		// This is needed because of https://github.com/creachadair/jrpc2/issues/118
		c.refreshClient()
	}
	return err
}

func (c *Client) GetEvents(ctx context.Context,
	request protocol.GetEventsRequest,
) (*protocol.GetEventsResponse, error) {
	var result protocol.GetEventsResponse
	err := c.callResult(ctx, protocol.GetEventsMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetFeeStats(ctx context.Context) (*protocol.GetFeeStatsResponse, error) {
	var result protocol.GetFeeStatsResponse
	err := c.callResult(ctx, protocol.GetFeeStatsMethodName, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetHealth(ctx context.Context) (*protocol.GetHealthResponse, error) {
	var result protocol.GetHealthResponse
	err := c.callResult(ctx, protocol.GetHealthMethodName, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetLatestLedger(ctx context.Context) (*protocol.GetLatestLedgerResponse, error) {
	var result protocol.GetLatestLedgerResponse
	err := c.callResult(ctx, protocol.GetLatestLedgerMethodName, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetLedgerEntries(ctx context.Context,
	request protocol.GetLedgerEntriesRequest,
) (*protocol.GetLedgerEntriesResponse, error) {
	var result protocol.GetLedgerEntriesResponse
	err := c.callResult(ctx, protocol.GetLedgerEntriesMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetLedgers(ctx context.Context,
	request protocol.GetLedgersRequest,
) (*protocol.GetLedgersResponse, error) {
	var result protocol.GetLedgersResponse
	err := c.callResult(ctx, protocol.GetLedgersMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetNetwork(ctx context.Context,
) (*protocol.GetNetworkResponse, error) {
	// phony
	var request protocol.GetNetworkRequest
	var result protocol.GetNetworkResponse
	err := c.callResult(ctx, protocol.GetNetworkMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetTransaction(ctx context.Context,
	request protocol.GetTransactionRequest,
) (*protocol.GetTransactionResponse, error) {
	var result protocol.GetTransactionResponse
	err := c.callResult(ctx, protocol.GetTransactionMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetTransactions(ctx context.Context,
	request protocol.GetTransactionsRequest,
) (*protocol.GetTransactionsResponse, error) {
	var result protocol.GetTransactionsResponse
	err := c.callResult(ctx, protocol.GetTransactionsMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) GetVersionInfo(ctx context.Context) (*protocol.GetVersionInfoResponse, error) {
	var result protocol.GetVersionInfoResponse
	err := c.callResult(ctx, protocol.GetVersionInfoMethodName, nil, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) SendTransaction(ctx context.Context,
	request protocol.SendTransactionRequest,
) (*protocol.SendTransactionResponse, error) {
	var result protocol.SendTransactionResponse
	err := c.callResult(ctx, protocol.SendTransactionMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) SimulateTransaction(ctx context.Context,
	request protocol.SimulateTransactionRequest,
) (*protocol.SimulateTransactionResponse, error) {
	var result protocol.SimulateTransactionResponse
	err := c.callResult(ctx, protocol.SimulateTransactionMethodName, request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
