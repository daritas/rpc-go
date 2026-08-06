package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	rpc "github.com/source-repo/rpc-go"
	rpcmqtt "github.com/source-repo/rpc-go/mqtt"
)

type Calculator struct{}

func (Calculator) add(a, b float64) (float64, error) {
	return a + b, nil
}

func main() {
	broker := env("MSGRPC_TEST_BROKER", "mqtt://localhost:1883")
	name := env("SOURCE_RPC_GO_EXAMPLE_NAME", "goCalculator")

	server := rpc.NewServer(name)
	server.SetNamespace("calculator", rpc.NamespaceOptions{Version: "1", ClassName: "Calculator"})
	server.DescribeEvent("calculator", rpc.EventDescription{
		Name:   "changed",
		Params: []rpc.TypeNode{rpc.TypeNumber()},
	})
	var transport *rpcmqtt.Transport
	server.ExposeFunc("calculator", "add", func(ctx context.Context, params []any) (any, error) {
		if len(params) != 2 {
			return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "calculator.add expects two numbers"}}
		}
		a, okA := number(params[0])
		b, okB := number(params[1])
		if !okA || !okB {
			return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "calculator.add expects two numbers"}}
		}
		result := a + b
		if transport != nil {
			_ = transport.PublishEvent(ctx, "calculator", "changed", []any{result})
		}
		return result, nil
	})
	server.DescribeMethod("calculator", rpc.MethodDescription{
		Name:    "add",
		Params:  []rpc.TypeNode{rpc.TypeNumber(), rpc.TypeNumber()},
		Returns: rpc.TypeNumber(),
	})

	transport = rpcmqtt.NewTransport(rpcmqtt.TransportOptions{
		URL:             broker,
		Name:            name,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	transport.AttachServer(server)
	if err := transport.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}
	log.Printf("%s serving calculator.add on %s", name, broker)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	<-done
	if err := transport.Close(context.Background()); err != nil {
		log.Print(err)
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func number(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	default:
		return 0, false
	}
}
