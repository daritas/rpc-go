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
