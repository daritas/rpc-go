package mqtt

import (
	"context"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func presencePublish(peer string, state string, shape string) *paho.Publish {
	packet := &paho.Publish{
		Topic:   "msgrpc/v2/presence/" + peer,
		Retain:  true,
		Payload: []byte(state),
	}
	if shape != "" {
		packet.Properties = &paho.PublishProperties{
			User: userProperties(map[string]string{shapeProperty: shape}),
		}
	}
	return packet
}

func watchingTransport(t *testing.T, events chan PresenceEvent) *Transport {
	t.Helper()
	return NewTransport(TransportOptions{
		Name:              "hmi",
		SubscribePresence: true,
		OnPresence: func(event PresenceEvent) {
			if events != nil {
				events <- event
			}
		},
	})
}

// The retained burst is peer discovery: a subscriber is handed everyone already online.
func TestRetainedPresenceBuildsThePeerList(t *testing.T) {
	transport := watchingTransport(t, nil)

	for _, peer := range []string{"plantServer", "gateway", "simulator"} {
		if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish(peer, PresenceOnline, "")}); err != nil {
			t.Fatal(err)
		}
	}

	peers := transport.Peers()
	if len(peers) != 3 {
		t.Fatalf("peers = %v, want three", peers)
	}
	// Sorted, so the list is stable to read and to assert on.
	if peers[0].Name != "gateway" || peers[1].Name != "plantServer" || peers[2].Name != "simulator" {
		t.Fatalf("peers = %v, want them sorted by name", peers)
	}
	if peers[0].Since.IsZero() {
		t.Fatal("Since was not stamped")
	}
}

func TestRetainedOfflineRemovesAPeer(t *testing.T) {
	events := make(chan PresenceEvent, 4)
	transport := watchingTransport(t, events)

	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOnline, "")}); err != nil {
		t.Fatal(err)
	}
	arrival := <-events
	if !arrival.Online || arrival.Peer != "plantServer" {
		t.Fatalf("event = %#v, want plantServer online", arrival)
	}

	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOffline, "")}); err != nil {
		t.Fatal(err)
	}
	departure := <-events
	if departure.Online || departure.Peer != "plantServer" {
		t.Fatalf("event = %#v, want plantServer offline", departure)
	}
	if peers := transport.Peers(); len(peers) != 0 {
		t.Fatalf("peers = %v, want none left", peers)
	}

	// An offline for a peer never seen online is not a change, and must not be reported as one.
	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("stranger", PresenceOffline, "")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("event = %#v, want nothing reported for an unknown peer going offline", event)
	case <-time.After(50 * time.Millisecond):
	}
}

// A peer that restarts with a changed surface is reported again, so a cached describe() answer for
// it can be discarded. A peer that restarts unchanged is not.
func TestChangedShapeIsReportedAndUnchangedIsNot(t *testing.T) {
	events := make(chan PresenceEvent, 4)
	transport := watchingTransport(t, events)

	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOnline, "abc123")}); err != nil {
		t.Fatal(err)
	}
	if event := <-events; event.Shape != "abc123" {
		t.Fatalf("event = %#v, want the announced shape", event)
	}

	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOnline, "abc123")}); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("event = %#v, want nothing reported for an unchanged shape", event)
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOnline, "def456")}); err != nil {
		t.Fatal(err)
	}
	if event := <-events; event.Shape != "def456" {
		t.Fatalf("event = %#v, want the changed shape", event)
	}
	if peers := transport.Peers(); len(peers) != 1 || peers[0].Shape != "def456" {
		t.Fatalf("peers = %v, want the changed shape recorded", peers)
	}
}

// A peer name off the wire is interpolated into topics and used as a map key, so a name that could
// change either is refused rather than recorded.
func TestUnusablePeerNamesAreRefused(t *testing.T) {
	rejected := make(chan RejectedFrame, 4)
	transport := NewTransport(TransportOptions{
		Name:              "hmi",
		SubscribePresence: true,
		OnRejected:        func(frame RejectedFrame) { rejected <- frame },
	})

	for _, name := range []string{"#", "+", "with\x00nul", "with\nnewline"} {
		packet := presencePublish(name, PresenceOnline, "")
		if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
			t.Fatal(err)
		}
		got := <-rejected
		if got.Reason != "unusable peer name on a presence topic" {
			t.Fatalf("rejected = %#v, want the name %q refused", got, name)
		}
	}
	if peers := transport.Peers(); len(peers) != 0 {
		t.Fatalf("peers = %v, want none recorded", peers)
	}
}

// A shape longer than the bound is dropped rather than stored, so a remote peer cannot grow this
// one's tables. The peer itself is still recorded.
func TestAnOverlongShapeIsIgnored(t *testing.T) {
	transport := watchingTransport(t, nil)
	long := ""
	for len(long) <= maxShapeLength {
		long += "a"
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("plantServer", PresenceOnline, long)}); err != nil {
		t.Fatal(err)
	}
	peers := transport.Peers()
	if len(peers) != 1 {
		t.Fatalf("peers = %v, want the peer still recorded", peers)
	}
	if peers[0].Shape != "" {
		t.Fatalf("shape = %q, want the overlong shape dropped", peers[0].Shape)
	}
}

// An empty network settles after one quiet period; a busy one settles after its burst stops.
func TestPresenceSettlesAfterTheBurstGoesQuiet(t *testing.T) {
	transport := watchingTransport(t, nil)
	transport.armPresenceSweep()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	if err := transport.PresenceSettled(ctx); err != nil {
		t.Fatalf("presence never settled: %v", err)
	}
	if elapsed := time.Since(start); elapsed < presenceQuietPeriod {
		t.Fatalf("settled after %s, want at least the quiet period", elapsed)
	}
	// Settled once and stays settled.
	if err := transport.PresenceSettled(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEachPresenceMessageRestartsTheQuietPeriod(t *testing.T) {
	transport := watchingTransport(t, nil)
	transport.armPresenceSweep()

	// Three messages spaced inside the quiet period: settling must wait for the last of them.
	for i := 0; i < 3; i++ {
		time.Sleep(presenceQuietPeriod / 2)
		if transport.presenceHasSettled() {
			t.Fatal("presence settled while the burst was still arriving")
		}
		if _, err := transport.onPublish(paho.PublishReceived{Packet: presencePublish("peer", PresenceOnline, "")}); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := transport.PresenceSettled(ctx); err != nil {
		t.Fatalf("presence never settled: %v", err)
	}
}

// A transport not watching presence has nothing coming, so waiting on it must not block.
func TestPresenceSettlesAtOnceWhenNotWatching(t *testing.T) {
	transport := NewTransport(TransportOptions{Name: "hmi"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := transport.PresenceSettled(ctx); err != nil {
		t.Fatalf("a transport not watching presence blocked: %v", err)
	}
}

func TestPresenceSettledHonoursItsContext(t *testing.T) {
	transport := watchingTransport(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	// Never armed, so nothing will settle it.
	if err := transport.PresenceSettled(ctx); err == nil {
		t.Fatal("PresenceSettled ignored its context")
	}
}

// A source off the wire keys the identity and subscription maps and names a topic this peer
// publishes to, so it is checked before any of that.
func TestUnusableSourceIsRefused(t *testing.T) {
	rejected := make(chan RejectedFrame, 1)
	transport := NewTransport(TransportOptions{
		Name:       "plant",
		OnRejected: func(frame RejectedFrame) { rejected <- frame },
	})
	packet := &paho.Publish{
		Topic:   "msgrpc/v2/req/plant",
		Payload: []byte("[]"),
		Properties: &paho.PublishProperties{
			ContentType:     "application/json",
			CorrelationData: []byte("request-1"),
			User: userProperties(map[string]string{
				MRVersion: FrameVersion,
				MRSource:  "#",
				MRKind:    string(KindCall),
				MRPath:    "plant",
				MRMethod:  "write",
			}),
		},
	}
	if _, err := transport.onPublish(paho.PublishReceived{Packet: packet}); err != nil {
		t.Fatal(err)
	}
	if got := <-rejected; got.Reason != "unusable "+MRSource {
		t.Fatalf("rejected = %#v, want the wildcard source refused", got)
	}
}
