package mqtt

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/source-repo/rpc-go"
)

func TestHMACSigningVerifiesCanonicalMQTTV5Frame(t *testing.T) {
	signer := NewHMACSigner("hmi-secret")
	verifier := NewHMACVerifier(map[string]string{"hmi": "hmi-secret"})
	frame := SignedFrameV5{
		Version:         FrameVersion,
		Topic:           "msgrpc/v2/req/plant",
		ResponseTopic:   "msgrpc/v2/rsp/hmi",
		Source:          "hmi",
		Kind:            "call",
		Path:            "plant",
		MethodOrEvent:   "write",
		Correlation:     "request-1",
		ContentType:     "application/json",
		ContractVersion: "1",
		TTL:             "4000",
		IdempotencyKey:  "work-order-42",
		Timestamp:       123456,
		Nonce:           "nonce",
		Payload:         []byte("1"),
	}
	canonical, err := CanonicalSignedBytesV5(frame)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := signer.Sign(context.Background(), canonical, SigningContext{Source: "hmi"})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := verifier.Verify(context.Background(), canonical, signature, SigningContext{Source: "hmi"})
	if err != nil {
		t.Fatal(err)
	}
	if identity == nil || identity.Name != "hmi" {
		t.Fatalf("identity = %#v, want hmi", identity)
	}

	frame.ContentType = "application/msgpack"
	tampered, err := CanonicalSignedBytesV5(frame)
	if err != nil {
		t.Fatal(err)
	}
	identity, err = verifier.Verify(context.Background(), tampered, signature, SigningContext{Source: "hmi"})
	if err != nil {
		t.Fatal(err)
	}
	if identity != nil {
		t.Fatalf("tampered frame verified as %#v", identity)
	}
}

func TestCanonicalSignedBytesV5MatchesJSONStringifyEscaping(t *testing.T) {
	canonical, err := CanonicalSignedBytesV5(SignedFrameV5{
		Version:       FrameVersion,
		Topic:         "msgrpc/v2/req/a&b",
		Source:        "hmi",
		Kind:          "call",
		Path:          "a&b",
		MethodOrEvent: "write",
		Timestamp:     123456,
		Nonce:         "nonce",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(canonical, []byte(`"a&b"`)) {
		t.Fatalf("canonical = %s, want unescaped ampersand like JSON.stringify", canonical)
	}
	if bytes.Contains(canonical, []byte(`\u0026`)) {
		t.Fatalf("canonical = %s, did not expect HTML escaping", canonical)
	}
}

func TestReplayGuardRejectsReplayAndStaleFrames(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	guard := NewReplayGuard(time.Second, 2)
	guard.now = func() time.Time { return now }

	if !guard.Accept("a", now.UnixMilli()) {
		t.Fatal("fresh nonce was rejected")
	}
	if guard.Accept("a", now.UnixMilli()) {
		t.Fatal("replayed nonce was accepted")
	}
	if guard.Accept("old", now.Add(-2*time.Second).UnixMilli()) {
		t.Fatal("stale nonce was accepted")
	}
	if !guard.Accept("b", now.UnixMilli()) || !guard.Accept("c", now.UnixMilli()) {
		t.Fatal("fresh nonce was rejected")
	}
	if guard.Size() > 2 {
		t.Fatalf("guard tracks %d nonces, want at most 2", guard.Size())
	}
}

func TestVerifierRejectsUnsignedMQTTV5Frame(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	transport := NewTransport(TransportOptions{
		Verifier:   NewHMACVerifier(map[string]string{"hmi": "hmi-secret"}),
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	packet := &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: []byte("[]"),
		Properties: &paho.PublishProperties{
			ContentType: (rpc.JSONCodec{}).ContentType(),
			User: userProperties(map[string]string{
				MRVersion: FrameVersion,
				MRSource:  "hmi",
				MRKind:    string(KindCall),
				MRPath:    "plant",
				MRMethod:  "write",
			}),
		},
	}
	properties, err := controlProperties(packet.Properties.User)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.verifyPublish(context.Background(), packet, properties); err == nil {
		t.Fatal("unsigned frame was accepted")
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	got := <-rejected
	if got.Source != "hmi" || got.Topic != packet.Topic || got.Reason != "unsigned" {
		t.Fatalf("rejected = %#v, want unsigned hmi frame", got)
	}
}

func TestTransportReportsRejectedReplay(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	transport := NewTransport(TransportOptions{
		Verifier:   NewHMACVerifier(map[string]string{"hmi": "hmi-secret"}),
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	packet := signedTestPublish(t, "hmi-secret")
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	got := <-rejected
	if got.Source != "hmi" || got.Reason != "stale or replayed" {
		t.Fatalf("rejected = %#v, want replay rejection", got)
	}
}

func TestTransportReportsDuplicateControlProperty(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	transport := NewTransport(TransportOptions{
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	properties := userProperties(map[string]string{
		MRVersion: FrameVersion,
		MRSource:  "hmi",
		MRKind:    string(KindCall),
		MRPath:    "plant",
		MRMethod:  "write",
	})
	properties.Add(MRKind, string(KindCall))
	packet := &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: []byte("[]"),
		Properties: &paho.PublishProperties{
			ContentType: (rpc.JSONCodec{}).ContentType(),
			User:        properties,
		},
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	got := <-rejected
	if got.Source != "hmi" || got.Reason != "duplicate control property "+MRKind {
		t.Fatalf("rejected = %#v, want duplicate property rejection", got)
	}
}

func TestTransportReportsUnsupportedContentTypeBeforeDecode(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	transport := NewTransport(TransportOptions{
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	packet := &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: []byte("[]"),
		Properties: &paho.PublishProperties{
			ContentType: "application/octet-stream",
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
	if got.Source != "hmi" || got.Reason != `unsupported content type "application/octet-stream"` {
		t.Fatalf("rejected = %#v, want unsupported content type rejection", got)
	}
}

func signedTestPublish(t *testing.T, secret string) *paho.Publish {
	t.Helper()
	body, err := (rpc.JSONCodec{}).Encode([]any{})
	if err != nil {
		t.Fatal(err)
	}
	topic := "msgrpc/v2/req/plant"
	responseTopic := "msgrpc/v2/rsp/hmi"
	timestamp := time.Now().UnixMilli()
	nonce := "test-nonce-" + rpc.NewID()
	canonical, err := CanonicalSignedBytesV5(SignedFrameV5{
		Version:       FrameVersion,
		Topic:         topic,
		ResponseTopic: responseTopic,
		Source:        "hmi",
		Kind:          string(KindCall),
		Path:          "plant",
		MethodOrEvent: "write",
		Correlation:   "request-1",
		ContentType:   (rpc.JSONCodec{}).ContentType(),
		Timestamp:     timestamp,
		Nonce:         nonce,
		Payload:       body,
	})
	if err != nil {
		t.Fatal(err)
	}
	signature, err := NewHMACSigner(secret).Sign(context.Background(), canonical, SigningContext{Source: "hmi"})
	if err != nil {
		t.Fatal(err)
	}
	return &paho.Publish{
		Topic:   topic,
		Payload: body,
		Properties: &paho.PublishProperties{
			ContentType:     (rpc.JSONCodec{}).ContentType(),
			ResponseTopic:   responseTopic,
			CorrelationData: []byte("request-1"),
			User: userProperties(map[string]string{
				MRVersion:   FrameVersion,
				MRSource:    "hmi",
				MRKind:      string(KindCall),
				MRPath:      "plant",
				MRMethod:    "write",
				MRNonce:     nonce,
				MRTimestamp: strconv.FormatInt(timestamp, 10),
				MRSignature: signature,
			}),
		},
	}
}
