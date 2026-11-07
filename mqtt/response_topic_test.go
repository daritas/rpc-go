package mqtt

import (
	"context"
	"strings"
	"testing"

	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/daritas/rpc-go"
)

func TestUnsafeTopicNamesAreRefused(t *testing.T) {
	unsafe := []string{
		"",
		"$SYS/broker/load",
		"msgrpc/v2/rsp/+",
		"msgrpc/v2/rsp/#",
		"msgrpc/v2/rsp/\x00hmi",
		"msgrpc/v2/rsp/\nhmi",
		strings.Repeat("a", maxTopicLength+1),
	}
	for _, topic := range unsafe {
		if isSafeTopicName(topic) {
			t.Fatalf("isSafeTopicName(%q) = true, want false", topic)
		}
	}
	if !isSafeTopicName("msgrpc/v2/rsp/hmi") {
		t.Fatal("a plain response topic was refused")
	}
}

func TestResponseTopicOutsideThePrefixIsRefused(t *testing.T) {
	transport := NewTransport(TransportOptions{Name: "plant"})
	reason := transport.refuseResponseTopic("somewhere/else/rsp/hmi", "hmi")
	if reason == "" {
		t.Fatal("a response topic outside the prefix was allowed")
	}
	if !strings.Contains(reason, "outside") {
		t.Fatalf("reason = %q, want it to say the topic is outside the prefix", reason)
	}
	if reason := transport.refuseResponseTopic("msgrpc/v2/rsp/hmi", "hmi"); reason != "" {
		t.Fatalf("a topic under the prefix was refused: %s", reason)
	}
}

func TestAllowResponseTopicReplacesThePrefixRule(t *testing.T) {
	transport := NewTransport(TransportOptions{
		Name: "plant",
		AllowResponseTopic: func(topic string, source string) bool {
			return source == "hmi" && topic == "elsewhere/rsp/hmi"
		},
	})
	if reason := transport.refuseResponseTopic("elsewhere/rsp/hmi", "hmi"); reason != "" {
		t.Fatalf("the allowed topic was refused: %s", reason)
	}
	if transport.refuseResponseTopic("elsewhere/rsp/hmi", "intruder") == "" {
		t.Fatal("the hook was bypassed for an unexpected source")
	}
	// The hook replaces the prefix rule; it does not replace the publishable-topic rules.
	if transport.refuseResponseTopic("$SYS/anything", "hmi") == "" {
		t.Fatal("a broker-reserved topic was allowed through the hook")
	}
}

func TestMissingResponseTopicIsDerivedFromTheSource(t *testing.T) {
	transport := NewTransport(TransportOptions{Name: "plant"})
	topic, reason := transport.resolveResponseTopic("", "hmi")
	if reason != "" {
		t.Fatalf("deriving a response topic failed: %s", reason)
	}
	if topic != "msgrpc/v2/rsp/hmi" {
		t.Fatalf("topic = %q, want msgrpc/v2/rsp/hmi", topic)
	}
	if _, reason := transport.resolveResponseTopic("", ""); reason == "" {
		t.Fatal("a request with no response topic and no source was accepted")
	}
}

// A request naming a hostile response topic must be refused before the method runs, not answered
// somewhere else afterwards: by the time a reply exists the side effect has already happened.
func TestHostileResponseTopicRefusesTheRequestBeforeDispatch(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	ran := make(chan struct{}, 1)

	server := rpc.NewServer("plant")
	server.ExposeFunc("plant", "write", func(context.Context, []any) (any, error) {
		ran <- struct{}{}
		return nil, nil
	})

	transport := NewTransport(TransportOptions{
		Name:       "plant",
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	transport.AttachServer(server)

	packet := &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: []byte("[]"),
		Properties: &paho.PublishProperties{
			ContentType:     (rpc.JSONCodec{}).ContentType(),
			ResponseTopic:   "$SYS/broker/load",
			CorrelationData: []byte("request-1"),
			User: userProperties(map[string]string{
				MRVersion: FrameVersion,
				MRSource:  "hmi",
				MRKind:    string(KindCall),
				MRPath:    "plant",
				MRMethod:  "write",
			}),
		},
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	got := <-rejected
	if got.Source != "hmi" || !strings.Contains(got.Reason, "$SYS") {
		t.Fatalf("rejected = %#v, want the $SYS response topic refused", got)
	}
	select {
	case <-ran:
		t.Fatal("the method ran for a request that was refused")
	default:
	}
}
