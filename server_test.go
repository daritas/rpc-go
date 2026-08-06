package rpc

import (
	"context"
	"testing"
	"time"
)

type calculator struct {
	memory int
}

func (c *calculator) Add(value int) (int, error) {
	c.memory += value
	return c.memory, nil
}

func (c *calculator) Read(ctx context.Context) int {
	_ = ctx
	return c.memory
}

func TestServerDispatchesReflectedMethods(t *testing.T) {
	server := NewServer("plantServer")
	if err := server.Expose("calculator", &calculator{}); err != nil {
		t.Fatal(err)
	}

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "calculator",
		Method: "Add",
		Params: []any{3},
	})

	success, ok := reply.(SuccessPayload)
	if !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	if success.Result != 3 {
		t.Fatalf("result = %v, want 3", success.Result)
	}
}

func TestServerRefusesExpiredCalls(t *testing.T) {
	server := NewServer("plantServer")
	server.ExposeFunc("plant", "Start", func(context.Context, []any) (any, error) {
		t.Fatal("expired call should not run")
		return nil, nil
	})

	reply := server.DispatchArrived(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "plant",
		Method: "Start",
		TTL:    1,
	}, time.Now().Add(-time.Second))

	failure, ok := reply.(ErrorPayload)
	if !ok {
		t.Fatalf("reply = %T, want ErrorPayload", reply)
	}
	if failure.Code != ErrorTimeout {
		t.Fatalf("code = %s, want %s", failure.Code, ErrorTimeout)
	}
}
