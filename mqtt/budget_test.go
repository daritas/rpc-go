package mqtt

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/daritas/rpc-go"
)

func TestRemainingTTLTakesBrokerExpiryAsAnUpperBound(t *testing.T) {
	expiry := func(seconds uint32) *uint32 { return &seconds }

	if got := remainingTTL(10000, expiry(4)); got != 4000 {
		t.Fatalf("remainingTTL = %d, want the broker's 4000 to narrow the stated 10000", got)
	}
	// Expiry may only shorten what the caller signed, never lengthen it.
	if got := remainingTTL(4000, expiry(30)); got != 4000 {
		t.Fatalf("remainingTTL = %d, want the stated 4000 to survive a longer expiry", got)
	}
	if got := remainingTTL(4000, nil); got != 4000 {
		t.Fatalf("remainingTTL = %d, want the stated ttl when no expiry is present", got)
	}
	if got := remainingTTL(0, expiry(30)); got != 0 {
		t.Fatalf("remainingTTL = %d, want no budget when the caller stated none", got)
	}
	if got := remainingTTL(10000, expiry(0)); got != 0 {
		t.Fatalf("remainingTTL = %d, want an exhausted expiry to leave nothing", got)
	}
}

// A request that spent its whole budget queued at the broker must not run. The broker reports what
// is left through the message expiry it hands on, which is the only clock either peer can trust.
func TestExpiredRequestIsAnsweredTimeoutWithoutRunningTheMethod(t *testing.T) {
	ran := make(chan struct{}, 1)
	server := rpc.NewServer("plant")
	server.ExposeFunc("plant", "write", func(context.Context, []any) (any, error) {
		ran <- struct{}{}
		return "written", nil
	})

	transport := NewTransport(TransportOptions{Name: "plant"})
	transport.AttachServer(server)

	frame, source, _, err := transport.decodePublish(requestPublish(t, 10000, 0))
	if err != nil {
		t.Fatal(err)
	}
	if frame.TTL != 0 {
		t.Fatalf("frame.TTL = %d, want an exhausted expiry to leave no budget", frame.TTL)
	}

	message, ok := FromFrame(frame)
	if !ok {
		t.Fatal("FromFrame returned false")
	}
	call := message.Payload.(rpc.CallPayload)
	// TTL 1ms, arrived well in the past: the shape of a request that waited out its budget.
	call.TTL = 1
	reply := server.DispatchArrivedWithIdentity(context.Background(), source, nil, call, time.Now().Add(-time.Second))

	failure, ok := reply.(rpc.ErrorPayload)
	if !ok {
		t.Fatalf("reply = %T, want rpc.ErrorPayload", reply)
	}
	if failure.Code != rpc.ErrorTimeout {
		t.Fatalf("code = %s, want %s", failure.Code, rpc.ErrorTimeout)
	}
	select {
	case <-ran:
		t.Fatal("the method ran for a request whose caller had stopped waiting")
	default:
	}
}

// The handler gets the caller's budget as a deadline, so a method cannot outlive the call it serves.
func TestHandlerContextCarriesTheCallersDeadline(t *testing.T) {
	deadlines := make(chan time.Time, 1)
	server := rpc.NewServer("plant")
	server.ExposeFunc("plant", "write", func(ctx context.Context, _ []any) (any, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			close(deadlines)
			return nil, nil
		}
		deadlines <- deadline
		return nil, nil
	})

	transport := NewTransport(TransportOptions{Name: "plant"})
	transport.AttachServer(server)

	arrived := time.Now()
	packet := requestPublish(t, 5000, 30)
	frame, source, _, err := transport.decodePublish(packet)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := FromFrame(frame)
	if !ok {
		t.Fatal("FromFrame returned false")
	}
	transport.answerRequest(source, packet, "msgrpc/v2/rsp/hmi", frame, message, arrived)

	select {
	case deadline, ok := <-deadlines:
		if !ok {
			t.Fatal("the handler ran without a deadline")
		}
		want := arrived.Add(5 * time.Second)
		if deadline.Sub(want) > time.Millisecond || want.Sub(deadline) > time.Millisecond {
			t.Fatalf("deadline = %s, want %s", deadline, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the handler never ran")
	}
}

// requestPublish builds a plain call frame stating ttl milliseconds, with the broker reporting
// expirySeconds left of it.
func requestPublish(t *testing.T, ttl int64, expirySeconds uint32) *paho.Publish {
	t.Helper()
	body, err := (rpc.JSONCodec{}).Encode([]any{})
	if err != nil {
		t.Fatal(err)
	}
	return &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: body,
		Properties: &paho.PublishProperties{
			ContentType:     (rpc.JSONCodec{}).ContentType(),
			ResponseTopic:   "msgrpc/v2/rsp/hmi",
			CorrelationData: []byte("request-1"),
			MessageExpiry:   &expirySeconds,
			User: userProperties(map[string]string{
				MRVersion: FrameVersion,
				MRSource:  "hmi",
				MRKind:    string(KindCall),
				MRPath:    "plant",
				MRMethod:  "write",
				MRTTL:     strconv.FormatInt(ttl, 10),
			}),
		},
	}
}
