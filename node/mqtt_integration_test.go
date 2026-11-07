package node

import (
	"context"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	rpc "github.com/daritas/rpc-go"
	rpcmqtt "github.com/daritas/rpc-go/mqtt"
)

func TestServiceRunsProgramOverSignedMQTTBroker(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireNodeBroker(t, nodeTCPAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefix := "msgrpc/go-node-test/" + rpc.NewID()
	secrets := map[string]string{
		"goProgramNode":   "node-secret",
		"goProgramClient": "client-secret",
	}
	options := ServiceOptions{
		Runner: RunnerOptions{
			Directory:      t.TempDir(),
			BuildTimeout:   20 * time.Second,
			StopTimeout:    time.Second,
			MaxOutputLines: 20,
		},
		Allow: []string{"goProgramClient"},
	}

	server := rpc.NewServer("goProgramNode")
	service := NewService(options)
	defer service.Close()
	if err := service.Expose(server); err != nil {
		t.Fatal(err)
	}
	server.UseAuthorizer(Authorizer(options, nil))

	serverTransport := rpcmqtt.NewTransport(rpcmqtt.TransportOptions{
		URL:      brokerURL,
		Name:     "goProgramNode",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:   rpcmqtt.NewHMACSigner(secrets["goProgramNode"]),
		Verifier: rpcmqtt.NewHMACVerifier(secrets),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipNodeBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := rpcmqtt.NewTransport(rpcmqtt.TransportOptions{
		URL:      brokerURL,
		Name:     "goProgramClient",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:   rpcmqtt.NewHMACSigner(secrets["goProgramClient"]),
		Verifier: rpcmqtt.NewHMACVerifier(secrets),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipNodeBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goProgramClient")
	client.Use(clientTransport)
	source := `package main

import "fmt"

func main() {
	fmt.Println("hello from mqtt")
}
`

	var saved ProgramFile
	if err := client.Call(ctx, "goProgramNode", Namespace, "saveProgram", []any{"hello", source}, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Name != "hello" {
		t.Fatalf("saved = %#v, want hello", saved)
	}

	var started StartResult
	if err := client.Call(ctx, "goProgramNode", Namespace, "startProgram", []any{"hello"}, &started); err != nil {
		t.Fatal(err)
	}
	if started.PID == 0 {
		t.Fatalf("started = %#v, want pid", started)
	}

	var output OutputResult
	waitForMQTTOutput(t, ctx, client, &output, func(output OutputResult) bool {
		return !output.Running && strings.Contains(strings.Join(output.Output, "\n"), "hello from mqtt")
	})
}

func waitForMQTTOutput(t *testing.T, ctx context.Context, client *rpc.Client, output *OutputResult, done func(OutputResult) bool) {
	t.Helper()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for MQTT program output, last = %#v: %v", *output, ctx.Err())
		case <-ticker.C:
			var next OutputResult
			if err := client.Call(ctx, "goProgramNode", Namespace, "programOutput", []any{"hello"}, &next); err != nil {
				t.Fatal(err)
			}
			*output = next
			if done(next) {
				return
			}
		}
	}
}

func nodeTCPAddress(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Port() == "" {
		return net.JoinHostPort(parsed.Hostname(), "1883")
	}
	return parsed.Host
}

func skipNodeBrokerConnect(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("SOURCE_RPC_REQUIRE_BROKER") == "1" {
		t.Fatal(err)
	}
	t.Skipf("MQTT broker rejected CONNECT; set MSGRPC_TEST_BROKER_USERNAME/PASSWORD if this broker requires auth: %v", err)
}

func requireNodeBroker(t *testing.T, address string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
	if err == nil {
		_ = conn.Close()
		return
	}
	if os.Getenv("SOURCE_RPC_REQUIRE_BROKER") == "1" {
		t.Fatalf("MQTT broker required but not reachable at %s: %v", address, err)
	}
	t.Skipf("MQTT broker not reachable at %s: %v", address, err)
}
