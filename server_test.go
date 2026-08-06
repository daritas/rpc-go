package rpc

import (
	"context"
	"errors"
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

func TestServerDescribesExposedMethods(t *testing.T) {
	server := NewServer("plantServer")
	if err := server.ExposeService("calculator", nil,
		WithNamespace(NamespaceVersion("1"), NamespaceClassName("Calculator")),
		WithMethods(Method("add", func(a, b int) (int, error) {
			return a + b, nil
		}, Params(TypeNumber(), TypeNumber()), Returns(TypeNumber()))),
	); err != nil {
		t.Fatal(err)
	}

	description := server.Describe()
	if description.Name != "plantServer" {
		t.Fatalf("name = %s, want plantServer", description.Name)
	}
	var calculator NamespaceDescription
	for _, namespace := range description.Namespaces {
		if namespace.Name == "calculator" {
			calculator = namespace
		}
	}
	if calculator.Name == "" {
		t.Fatal("calculator namespace missing")
	}
	if calculator.Version != "1" {
		t.Fatalf("version = %s, want 1", calculator.Version)
	}
	if len(calculator.Methods) != 1 || calculator.Methods[0].Name != "add" {
		t.Fatalf("methods = %#v, want add", calculator.Methods)
	}
	if calculator.Methods[0].Returns["kind"] != "number" {
		t.Fatalf("returns = %#v, want number", calculator.Methods[0].Returns)
	}
}

func TestExposeServiceDispatchesTypedFunction(t *testing.T) {
	server := NewServer("plantServer")
	if err := server.ExposeService("calculator", nil,
		WithMethods(Method("add", func(a, b int) (int, error) {
			return 5, nil
		}, Params(TypeNumber(), TypeNumber()), Returns(TypeNumber()))),
	); err != nil {
		t.Fatal(err)
	}

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "calculator",
		Method: "add",
		Params: []any{2, 3},
	})
	success, ok := reply.(SuccessPayload)
	if !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	if success.Result != 5 {
		t.Fatalf("result = %v, want 5", success.Result)
	}
}

func TestServerDispatchesDescribe(t *testing.T) {
	server := NewServer("plantServer")
	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "msgrpc",
		Method: "describe",
		Params: []any{},
	})

	success, ok := reply.(SuccessPayload)
	if !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	description, ok := success.Result.(ServerDescription)
	if !ok {
		t.Fatalf("result = %T, want ServerDescription", success.Result)
	}
	if description.Name != "plantServer" {
		t.Fatalf("name = %s, want plantServer", description.Name)
	}
}

func TestExposeFunctionDispatchesWithDescription(t *testing.T) {
	server := NewServer("plantServer")
	server.ExposeFunction("calculator", "add", func(a, b int) (int, error) {
		return a + b, nil
	}, Params(TypeNumber(), TypeNumber()), Returns(TypeNumber()))

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "calculator",
		Method: "add",
		Params: []any{2, 3},
	})
	success, ok := reply.(SuccessPayload)
	if !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	if success.Result != 5 {
		t.Fatalf("result = %v, want 5", success.Result)
	}
	method := server.Describe().Namespaces[0].Methods[0]
	if method.Name != "add" || method.Returns["kind"] != "number" {
		t.Fatalf("method = %#v, want described add", method)
	}
}

type receiverCalculator struct{}

func (receiverCalculator) Add(a, b int) (int, error) {
	return a + b, nil
}

func TestExposeServiceReceiverMethodWithWireName(t *testing.T) {
	server := NewServer("plantServer")
	if err := server.ExposeService("calculator", receiverCalculator{},
		WithNamespace(NamespaceVersion("1")),
		WithMethods(ReceiverMethod("Add", WireName("add"), Params(TypeNumber(), TypeNumber()), Returns(TypeNumber()))),
	); err != nil {
		t.Fatal(err)
	}
	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "calculator",
		Method: "add",
		Params: []any{2, 3},
	})
	success, ok := reply.(SuccessPayload)
	if !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	if success.Result != 5 {
		t.Fatalf("result = %v, want 5", success.Result)
	}
}

func TestServerCachesNonRepeatableCommandOutcome(t *testing.T) {
	server := NewServer("plantServer")
	server.UseIdempotency(NewMemoryIdempotencyStore(10))
	runs := 0
	server.ExposeMethod("gate", "startPump", func(context.Context, []any) (any, error) {
		runs++
		return "start 1", nil
	}, Semantics(SemanticsNonRepeatableCommand))

	first := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:             "request-1",
		Type:           RPCMessageCall,
		Path:           "gate",
		Method:         "startPump",
		IdempotencyKey: "work-order-42",
	})
	second := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:             "request-2",
		Type:           RPCMessageCall,
		Path:           "gate",
		Method:         "startPump",
		IdempotencyKey: "work-order-42",
	})

	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
	if first.(SuccessPayload).Result != "start 1" {
		t.Fatalf("first = %#v, want cached result", first)
	}
	success, ok := second.(SuccessPayload)
	if !ok {
		t.Fatalf("second = %T, want SuccessPayload", second)
	}
	if success.ID != "request-2" || success.Result != "start 1" {
		t.Fatalf("second = %#v, want cached result with new request id", success)
	}
}

func TestServerDoesNotUseIdempotencyForQueries(t *testing.T) {
	store := &recordingStore{}
	server := NewServer("plantServer")
	server.UseIdempotency(store)
	server.ExposeMethod("gate", "readMode", func(context.Context, []any) (any, error) {
		return "auto", nil
	}, Semantics(SemanticsQuery))

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "gate",
		Method: "readMode",
	})
	if _, ok := reply.(SuccessPayload); !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
	if store.begun != 0 {
		t.Fatalf("begin called %d times, want 0", store.begun)
	}
}

func TestServerRefusesCommandWhenIdempotencyStoreFails(t *testing.T) {
	store := &recordingStore{err: errors.New("store down")}
	server := NewServer("plantServer")
	server.UseIdempotency(store)
	ran := false
	server.ExposeMethod("gate", "startPump", func(context.Context, []any) (any, error) {
		ran = true
		return "started", nil
	}, Semantics(SemanticsNonRepeatableCommand))

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "gate",
		Method: "startPump",
	})
	failure, ok := reply.(ErrorPayload)
	if !ok {
		t.Fatalf("reply = %T, want ErrorPayload", reply)
	}
	if failure.Code != ErrorUnknownOutcome {
		t.Fatalf("code = %s, want %s", failure.Code, ErrorUnknownOutcome)
	}
	if ran {
		t.Fatal("handler ran even though the idempotency store failed")
	}
}

func TestServerDropsInProgressDuplicate(t *testing.T) {
	store := &recordingStore{decision: IdempotencyInProgress}
	server := NewServer("plantServer")
	server.UseIdempotency(store)
	server.ExposeMethod("gate", "startPump", func(context.Context, []any) (any, error) {
		t.Fatal("in-progress duplicate should not run")
		return nil, nil
	}, Semantics(SemanticsNonRepeatableCommand))

	reply := server.Dispatch(context.Background(), "hmi", CallPayload{
		ID:     "request-1",
		Type:   RPCMessageCall,
		Path:   "gate",
		Method: "startPump",
	})
	if reply != nil {
		t.Fatalf("reply = %#v, want nil", reply)
	}
}

type recordingStore struct {
	begun    int
	decision IdempotencyDecision
	outcome  StoredOutcome
	err      error
}

func (s *recordingStore) Begin(context.Context, Invocation) (StoredOutcome, IdempotencyDecision, error) {
	s.begun++
	if s.err != nil {
		return StoredOutcome{}, IdempotencyAcquired, s.err
	}
	if s.decision == 0 {
		return StoredOutcome{}, IdempotencyAcquired, nil
	}
	return s.outcome, s.decision, nil
}

func (s *recordingStore) Complete(context.Context, Invocation, StoredOutcome) error {
	return nil
}
