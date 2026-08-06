package mqtt

import (
	"context"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	rpc "github.com/source-repo/rpc-go"
)

type integrationCalculator struct{}

func (integrationCalculator) Add(a, b int) (int, error) {
	return a + b, nil
}

func TestTransportCallAgainstBroker(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "msgrpc/go-test/" + rpc.NewClient("ids").NewCall("x", "y", nil, rpc.CallOptions{}).ID

	server := rpc.NewServer("goServer")
	if err := server.Expose("calculator", integrationCalculator{}); err != nil {
		t.Fatal(err)
	}
	serverTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goServer",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goClient",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goClient")
	client.Use(clientTransport)

	var result int
	if err := client.Call(ctx, "goServer", "calculator", "Add", []any{2, 3}, &result); err != nil {
		t.Fatal(err)
	}
	if result != 5 {
		t.Fatalf("result = %d, want 5", result)
	}
}

func TestTransportDeliversEvents(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "msgrpc/go-test/" + rpc.NewID()

	server := rpc.NewServer("goEventServer")
	server.SetNamespace("calculator", rpc.NamespaceOptions{Version: "1", ClassName: "Calculator", Emitter: true})
	server.DescribeEvent("calculator", rpc.EventDescription{
		Name:   "changed",
		Params: []rpc.TypeNode{rpc.TypeNumber()},
	})
	serverTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goEventServer",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goEventClient",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goEventClient")
	client.Use(clientTransport)
	received := make(chan rpc.EventPayload, 1)
	unsubscribe, err := client.Subscribe(ctx, "goEventServer", "calculator", "changed", func(event rpc.EventPayload) {
		received <- event
	})
	if err != nil {
		t.Fatal(err)
	}
	defer unsubscribe(context.Background())

	if err := serverTransport.PublishEvent(ctx, "calculator", "changed", []any{42}); err != nil {
		t.Fatal(err)
	}

	select {
	case event := <-received:
		if event.Path != "calculator" || event.Event != "changed" {
			t.Fatalf("event = %#v, want calculator.changed", event)
		}
		if len(event.Params) != 1 {
			t.Fatalf("params = %#v, want one value", event.Params)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestTransportRequiresIdentityForProtectedEventSubscription(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "msgrpc/go-test/" + rpc.NewID()

	server := rpc.NewServer("goProtectedEventServer")
	server.SetNamespace("calculator", rpc.NamespaceOptions{Version: "1", ClassName: "Calculator", Emitter: true})
	server.DescribeEvent("calculator", rpc.EventDescription{
		Name:              "changed",
		Params:            []rpc.TypeNode{rpc.TypeNumber()},
		RequiresAuthority: true,
	})
	serverTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goProtectedEventServer",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goProtectedEventClient",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goProtectedEventClient")
	client.Use(clientTransport)
	_, err := client.Subscribe(ctx, "goProtectedEventServer", "calculator", "changed", func(rpc.EventPayload) {})
	if err == nil {
		t.Fatal("Subscribe returned nil error, want Unauthorized")
	}
	rpcErr, ok := err.(*rpc.RPCError)
	if !ok || rpcErr.Code != rpc.ErrorUnauthorized {
		t.Fatalf("err = %#v, want Unauthorized RPCError", err)
	}
}

func TestTransportAuthorizerCanDenyEventSubscription(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "msgrpc/go-test/" + rpc.NewID()

	server := rpc.NewServer("goDeniedEventServer")
	server.SetNamespace("calculator", rpc.NamespaceOptions{Version: "1", ClassName: "Calculator", Emitter: true})
	server.DescribeEvent("calculator", rpc.EventDescription{Name: "changed"})
	sawSubscription := make(chan struct{}, 1)
	server.UseAuthorizer(func(_ context.Context, callContext rpc.CallContext) (bool, error) {
		if callContext.Subscription && callContext.InstanceName == "calculator" && callContext.Method == "changed" {
			select {
			case sawSubscription <- struct{}{}:
			default:
			}
			return false, nil
		}
		return true, nil
	})
	serverTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goDeniedEventServer",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goDeniedEventClient",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goDeniedEventClient")
	client.Use(clientTransport)
	_, err := client.Subscribe(ctx, "goDeniedEventServer", "calculator", "changed", func(rpc.EventPayload) {})
	if err == nil {
		t.Fatal("Subscribe returned nil error, want Forbidden")
	}
	rpcErr, ok := err.(*rpc.RPCError)
	if !ok || rpcErr.Code != rpc.ErrorForbidden {
		t.Fatalf("err = %#v, want Forbidden RPCError", err)
	}
	select {
	case <-sawSubscription:
	default:
		t.Fatal("authorizer did not see subscription context")
	}
}

func TestTransportSignedCallAgainstBroker(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	prefix := "msgrpc/go-test/" + rpc.NewID()
	secrets := map[string]string{
		"goSignedServer": "server-secret",
		"goSignedClient": "client-secret",
	}

	server := rpc.NewServer("goSignedServer")
	server.ExposeFunction("auth", "who", func(ctx context.Context) (string, error) {
		callContext, ok := rpc.CallContextFromContext(ctx)
		if !ok || callContext.Identity == nil {
			return "", &rpc.RPCError{Code: rpc.ErrorUnauthorized, Err: &rpc.RemoteError{Name: "RpcError", Message: "missing identity"}}
		}
		return callContext.Identity.Name, nil
	}, rpc.RequiresAuthority(true))
	serverVerifier := NewHMACVerifier(secrets)
	serverTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goSignedServer",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:          NewHMACSigner(secrets["goSignedServer"]),
		Verifier:        serverVerifier,
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	clientTransport := NewTransport(TransportOptions{
		URL:             brokerURL,
		Name:            "goSignedClient",
		Prefix:          prefix,
		PublishPresence: true,
		Username:        os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password:        os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:          NewHMACSigner(secrets["goSignedClient"]),
		Verifier:        NewHMACVerifier(secrets),
	})
	if err := clientTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer clientTransport.Close(context.Background())

	client := rpc.NewClient("goSignedClient")
	client.Use(clientTransport)

	var result string
	if err := client.Call(ctx, "goSignedServer", "auth", "who", nil, &result); err != nil {
		t.Fatal(err)
	}
	if result != "goSignedClient" {
		t.Fatalf("result = %s, want goSignedClient", result)
	}
	identity, ok := serverTransport.Identity("goSignedClient")
	if !ok || identity.Name != "goSignedClient" {
		t.Fatalf("identity = %#v, %v; want goSignedClient", identity, ok)
	}
}

func TestTransportCallsSourceRPCPeer(t *testing.T) {
	peer := os.Getenv("MSGRPC_TEST_INTEROP_PEER")
	if peer == "" {
		t.Skip("set MSGRPC_TEST_INTEROP_PEER to call a live Source RPC peer")
	}
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goInteropClient",
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
	})
	if err := transport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer transport.Close(context.Background())

	client := rpc.NewClient("goInteropClient")
	client.Use(transport)

	var result float64
	if err := client.Call(ctx, peer, "calculator", "add", []any{2, 3}, &result); err != nil {
		t.Fatal(err)
	}
	if result != 5 {
		t.Fatalf("result = %v, want 5", result)
	}
}

func tcpAddress(t *testing.T, raw string) string {
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

func skipBrokerConnect(t *testing.T, err error) {
	t.Helper()
	if os.Getenv("SOURCE_RPC_REQUIRE_BROKER") == "1" {
		t.Fatal(err)
	}
	t.Skipf("MQTT broker rejected CONNECT; set MSGRPC_TEST_BROKER_USERNAME/PASSWORD if this broker requires auth: %v", err)
}

func requireBroker(t *testing.T, address string) {
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
