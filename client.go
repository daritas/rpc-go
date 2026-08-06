package rpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

const DefaultCallTimeout = 10 * time.Second

type CallOptions struct {
	ID             string
	Version        string
	Timeout        time.Duration
	IdempotencyKey string
	OwnerEpoch     string
}

type EventHandler func(EventPayload)

type Client struct {
	name      string
	timeout   time.Duration
	transport Caller
}

func NewClient(name string) *Client {
	return &Client{name: name, timeout: DefaultCallTimeout}
}

func (c *Client) Name() string {
	return c.name
}

type Caller interface {
	Call(ctx context.Context, source string, target string, call CallPayload) (any, error)
}

type Subscriber interface {
	Subscribe(ctx context.Context, source string, target string, path string, event string, handler EventHandler) (func(context.Context) error, error)
}

func (c *Client) Use(transport Caller) {
	c.transport = transport
}

func (c *Client) Call(ctx context.Context, target, path, method string, params []any, result any, options ...CallOptions) error {
	if c.transport == nil {
		return &RPCError{Code: ErrorTransport, Err: &RemoteError{Name: "RpcError", Message: "no transport configured"}}
	}
	var callOptions CallOptions
	if len(options) > 0 {
		callOptions = options[0]
	}
	call := c.NewCall(path, method, params, callOptions)
	value, err := c.transport.Call(ctx, c.name, target, call)
	if err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, result)
}

func (c *Client) Subscribe(ctx context.Context, target, path, event string, handler EventHandler) (func(context.Context) error, error) {
	subscriber, ok := c.transport.(Subscriber)
	if !ok {
		return nil, &RPCError{Code: ErrorTransport, Err: &RemoteError{Name: "RpcError", Message: "transport does not support subscriptions"}}
	}
	return subscriber.Subscribe(ctx, c.name, target, path, event, handler)
}

func (c *Client) NewCall(path, method string, params []any, options CallOptions) CallPayload {
	id := options.ID
	if id == "" {
		id = NewID()
	}
	timeout := options.Timeout
	if timeout == 0 {
		timeout = c.timeout
	}
	call := CallPayload{
		ID:             id,
		Type:           RPCMessageCall,
		Path:           path,
		Method:         method,
		Params:         params,
		Version:        options.Version,
		IdempotencyKey: options.IdempotencyKey,
	}
	if timeout > 0 {
		call.TTL = int64(timeout / time.Millisecond)
	}
	if options.OwnerEpoch != "" {
		call.Fence = &OwnerFence{OwnerEpoch: options.OwnerEpoch}
	}
	return call
}

func NewID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return hex.EncodeToString(bytes[:])
}
