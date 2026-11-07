package mqtt

import (
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/daritas/rpc-go"
)

func TestSubscriptionTopicsCoverEveryChannel(t *testing.T) {
	transport := NewTransport(TransportOptions{Name: "plant"})
	want := []string{"msgrpc/v2/rsp/plant", "msgrpc/v2/evt/plant", "msgrpc/v2/req/plant"}
	if got := transport.subscriptionTopics(); !reflect.DeepEqual(got, want) {
		t.Fatalf("topics = %v, want %v", got, want)
	}

	watching := NewTransport(TransportOptions{Name: "plant", SubscribePresence: true})
	if got := watching.subscriptionTopics(); len(got) != 4 || got[3] != "msgrpc/v2/presence/+" {
		t.Fatalf("topics = %v, want a presence subscription appended", got)
	}
}

// A SUBACK reason code at or above 0x80 is a refusal, and the subscribe call itself still succeeds.
// Reading the codes is the only way to notice this peer came back deaf on a topic.
func TestRefusedSubscriptionsReadsSubackReasonCodes(t *testing.T) {
	topics := []string{"msgrpc/v2/rsp/plant", "msgrpc/v2/evt/plant", "msgrpc/v2/req/plant"}

	granted := &paho.Suback{Reasons: []byte{0x01, 0x00, 0x01}}
	if refused := refusedSubscriptions(topics, granted); refused != nil {
		t.Fatalf("refused = %v, want none when every code is a granted QoS", refused)
	}

	denied := &paho.Suback{Reasons: []byte{0x01, 0x87, 0x00}}
	want := []string{"msgrpc/v2/evt/plant"}
	if refused := refusedSubscriptions(topics, denied); !reflect.DeepEqual(refused, want) {
		t.Fatalf("refused = %v, want %v", refused, want)
	}

	// A broker answering with fewer codes than topics has not granted the remainder.
	short := &paho.Suback{Reasons: []byte{0x01}}
	if refused := refusedSubscriptions(topics, short); len(refused) != 2 {
		t.Fatalf("refused = %v, want the two unanswered topics", refused)
	}
}

// A dropped connection fails the calls that were waiting on it, rather than leaving them to time
// out one by one, and says so through OnConnection.
func TestConnectionDownFailsPendingCallsAndReports(t *testing.T) {
	events := make(chan ConnectionEvent, 2)
	transport := NewTransport(TransportOptions{
		Name:         "hmi",
		OnConnection: func(event ConnectionEvent) { events <- event },
	})

	wait := make(chan response, 1)
	transport.mu.Lock()
	transport.pending["request-1"] = wait
	transport.connected = true
	transport.mu.Unlock()

	if !transport.onConnectionDown() {
		t.Fatal("onConnectionDown asked the manager to stop reconnecting while the transport is open")
	}

	select {
	case reply := <-wait:
		var failure *rpc.RPCError
		if !errors.As(reply.err, &failure) {
			t.Fatalf("err = %v, want an *rpc.RPCError", reply.err)
		}
		if failure.Code != rpc.ErrorTransport {
			t.Fatalf("code = %s, want %s", failure.Code, rpc.ErrorTransport)
		}
	case <-time.After(time.Second):
		t.Fatal("the pending call was never failed")
	}

	event := <-events
	if event.Up {
		t.Fatalf("event = %#v, want a connection-down report", event)
	}
}

// Once Close has been called the manager is told to stop, so a deliberate shutdown does not turn
// into a reconnect loop.
func TestConnectionDownStopsReconnectingAfterClose(t *testing.T) {
	transport := NewTransport(TransportOptions{Name: "hmi"})
	transport.mu.Lock()
	transport.closed = true
	transport.mu.Unlock()

	if transport.onConnectionDown() {
		t.Fatal("onConnectionDown kept reconnecting after Close")
	}
}

// Retrying is worth something against a broker that failed to answer, and worth nothing against one
// that refused who this peer says it is.
func TestOnlyIdentityRefusalsAreTreatedAsFinal(t *testing.T) {
	final := []byte{0x85, 0x86, 0x87, 0x8A}
	for _, code := range final {
		err := &autopaho.ConnackError{ReasonCode: code, Err: errors.New("denied")}
		if !isRefusedIdentity(err) {
			t.Fatalf("reason code %#x was not treated as final", code)
		}
	}
	// Server unavailable is worth another attempt.
	retryable := &autopaho.ConnackError{ReasonCode: 0x88, Err: errors.New("unavailable")}
	if isRefusedIdentity(retryable) {
		t.Fatal("a server-unavailable connack was treated as final")
	}
	if isRefusedIdentity(errors.New("dial tcp: connection refused")) {
		t.Fatal("a dial failure was treated as final")
	}
	if isRefusedIdentity(nil) {
		t.Fatal("a nil error was treated as final")
	}
}

func TestBrokerURLFillsInTheDefaultPort(t *testing.T) {
	parsed, err := brokerURL("mqtt://broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Host != net.JoinHostPort("broker.local", "1883") {
		t.Fatalf("host = %q, want the default port filled in", parsed.Host)
	}

	kept, err := brokerURL("mqtt://broker.local:1884")
	if err != nil {
		t.Fatal(err)
	}
	if kept.Port() != "1884" {
		t.Fatalf("port = %q, want the stated port kept", kept.Port())
	}

	// Scheme handling and the TLS port default are covered in tls_test.go.
	if _, err := brokerURL("mqtt://"); err == nil {
		t.Fatal("a URL with no host was accepted")
	}
}
