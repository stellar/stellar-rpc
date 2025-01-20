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
	err := c.callResult(ctx, "getEvents", request, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
