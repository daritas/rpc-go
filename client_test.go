package rpc

import (
	"context"
	"testing"
	"time"
)

type blockingCaller struct {
	seen chan CallPayload
}

func (c blockingCaller) Call(ctx context.Context, _ string, _ string, call CallPayload) (any, error) {
	c.seen <- call
	<-ctx.Done()
	return nil, &RPCError{Code: ErrorTimeout, Err: &RemoteError{Name: "RpcError", Message: ctx.Err().Error()}}
}

func TestClientAppliesDefaultTimeoutLocally(t *testing.T) {
	caller := blockingCaller{seen: make(chan CallPayload, 1)}
	client := NewClient("hmi")
	client.timeout = 20 * time.Millisecond
	client.Use(caller)

	start := time.Now()
	err := client.Call(context.Background(), "plant", "calculator", "Read", nil, nil)
	if err == nil {
		t.Fatal("Call returned nil error, want timeout")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Call took %s, want local timeout", elapsed)
	}
	call := <-caller.seen
	if call.TTL < 20 || call.TTL > 50 {
		t.Fatalf("TTL = %d, want about 20 ms", call.TTL)
	}
}

func TestClientAdvertisesParentContextDeadline(t *testing.T) {
	caller := blockingCaller{seen: make(chan CallPayload, 1)}
	client := NewClient("hmi")
	client.timeout = time.Second
	client.Use(caller)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_ = client.Call(ctx, "plant", "calculator", "Read", nil, nil)

	call := <-caller.seen
	if call.TTL < 1 || call.TTL > 100 {
		t.Fatalf("TTL = %d, want parent deadline budget", call.TTL)
	}
}
