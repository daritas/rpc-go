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

func main() {
	broker := env("MSGRPC_TEST_BROKER", "mqtt://localhost:1883")
	name := env("SOURCE_RPC_GO_EXAMPLE_NAME", "goCalculator")

	server := rpc.NewServer(name)
	var transport *rpcmqtt.Transport
	if err := server.ExposeService("calculator", nil,
		rpc.WithNamespace(rpc.NamespaceVersion("1"), rpc.NamespaceClassName("Calculator"), rpc.NamespaceEmitter(true)),
		rpc.WithEvents(rpc.Event("changed", rpc.TypeNumber())),
		rpc.WithMethods(rpc.Method("add", func(ctx context.Context, a, b float64) (float64, error) {
			result := a + b
			if transport != nil {
				_ = transport.PublishEvent(ctx, "calculator", "changed", []any{result})
			}
			return result, nil
		}, rpc.Params(rpc.TypeNumber(), rpc.TypeNumber()), rpc.Returns(rpc.TypeNumber()))),
	); err != nil {
		log.Fatal(err)
	}

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
