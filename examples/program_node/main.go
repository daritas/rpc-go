package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	rpc "github.com/source-repo/rpc-go"
	rpcmqtt "github.com/source-repo/rpc-go/mqtt"
	"github.com/source-repo/rpc-go/node"
)

func main() {
	broker := env("SOURCE_RPC_BROKER", env("MSGRPC_TEST_BROKER", "mqtt://localhost:1883"))
	name := env("SOURCE_RPC_GO_EXAMPLE_NAME", "goProgramNode")
	programs := env("SOURCE_RPC_PROGRAMS", "programs")
	options := node.ServiceOptions{
		Runner: node.RunnerOptions{Directory: programs},
		Allow:  splitCSV(os.Getenv("SOURCE_RPC_SCRIPTABLE_BY")),
	}

	server := rpc.NewServer(name)
	service := node.NewService(options)
	defer service.Close()
	if err := service.Expose(server); err != nil {
		log.Fatal(err)
	}
	if len(options.Allow) > 0 {
		server.UseAuthorizer(node.Authorizer(options, nil))
	}
	signer, verifier := hmacAuth(os.Getenv("SOURCE_RPC_SHARED_SECRET"))

	transport := rpcmqtt.NewTransport(rpcmqtt.TransportOptions{
		URL:             broker,
		Name:            name,
		PublishPresence: true,
		Username:        env("SOURCE_RPC_BROKER_USERNAME", os.Getenv("MSGRPC_TEST_BROKER_USERNAME")),
		Password:        env("SOURCE_RPC_BROKER_PASSWORD", os.Getenv("MSGRPC_TEST_BROKER_PASSWORD")),
		Signer:          signer,
		Verifier:        verifier,
	})
	transport.AttachServer(server)
	if err := transport.Connect(context.Background()); err != nil {
		log.Fatal(err)
	}
	log.Printf("%s serving %s from %s on %s", name, node.Namespace, programs, broker)

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

func splitCSV(value string) []string {
	fields := strings.Split(value, ",")
	out := make([]string, 0, len(fields))
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field != "" {
			out = append(out, field)
		}
	}
	return out
}

func hmacAuth(secret string) (rpcmqtt.MessageSigner, rpcmqtt.MessageVerifier) {
	if secret == "" {
		return nil, nil
	}
	return rpcmqtt.NewHMACSigner(secret), rpcmqtt.HMACVerifier{
		Resolve: func(source string) ([]byte, bool) {
			if source == "" {
				return nil, false
			}
			return []byte(secret), true
		},
	}
}
