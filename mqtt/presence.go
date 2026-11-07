package mqtt

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

// presenceQuietPeriod is how long the presence topic must stay silent before the retained burst
// counts as delivered. Matches the TypeScript transport's sweep, so both settle alike.
const presenceQuietPeriod = 250 * time.Millisecond

// shapeProperty carries a peer's description hash beside its retained `online`.
//
// Beside rather than inside: the payload has to stay the exact string every deployed peer compares
// against, and changing it would make this peer invisible to them. An MQTT 5 user property is what
// there is for data an older reader never looks at.
const shapeProperty = "shape"

// maxShapeLength bounds a description hash arriving from the network. Generous — the library's own
// hashes are 16 hex characters — because the field is only ever compared for equality, so all that
// matters is that a remote peer cannot grow this one's tables.
const maxShapeLength = 64

// Peer is what presence says about a peer on the broker.
type Peer struct {
	// Name is the peer name, which is also the topic level its channels sit under.
	Name string
	// Shape is the description hash the peer announced, empty when it announced none. A change
	// means the peer's surface changed, so a cached describe() answer for it is stale.
	Shape string
	// Since is when this peer was first seen online by this transport.
	Since time.Time
}

// PresenceEvent reports a peer arriving or leaving.
type PresenceEvent struct {
	Peer   string
	Online bool
	// Shape is the peer's description hash on an arrival, when it announced one.
	Shape string
}

// Peers is every peer presence has reported online, sorted by name.
//
// Includes this peer when it publishes its own presence: this is what the broker says is there, not
// a filtered view, and quietly omitting ourselves would be a surprise to anyone comparing the list
// against what MQTT Explorer shows.
func (t *Transport) Peers() []Peer {
	t.mu.Lock()
	peers := make([]Peer, 0, len(t.peers))
	for _, peer := range t.peers {
		peers = append(peers, peer)
	}
	t.mu.Unlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	return peers
}

// PresenceSettled waits until the retained presence burst has been read.
//
// The broker sends everything retained under <prefix>/presence/+ the moment the subscription is
// granted, and there is no "that was everyone" packet — a short silence after the grant is the only
// end marker MQTT has. So the gap is armed when the subscribe is acknowledged and restarted by each
// presence message: an empty network settles after one quiet period, a full one after its burst.
//
// A heuristic with an honest name. A broker slower than the gap loses the race, which is what the
// caller's own context is for. A transport not watching presence settles at once, because nothing
// is coming.
func (t *Transport) PresenceSettled(ctx context.Context) error {
	if !t.options.SubscribePresence {
		return nil
	}
	select {
	case <-t.settled:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handlePresence records a retained presence message and restarts the quiet period.
func (t *Transport) handlePresence(packet *paho.Publish) {
	// Armed whatever the message turns out to be: it is traffic on the presence topic, which is what
	// the quiet period measures the absence of.
	defer t.armPresenceSweep()

	name := strings.TrimPrefix(packet.Topic, t.options.Prefix+"/presence/")
	if !isSafeTopicSegment(name) {
		t.reject(packet, name, "unusable peer name on a presence topic", nil)
		return
	}

	state := string(packet.Payload)
	var event PresenceEvent
	changed := false

	t.mu.Lock()
	switch state {
	case PresenceOffline:
		if _, known := t.peers[name]; known {
			delete(t.peers, name)
			// The identity was learned from a signature this peer sent; the peer is now gone, and a
			// later frame claiming its name has to earn the identity again.
			delete(t.peerIdentities, name)
			changed = true
			event = PresenceEvent{Peer: name, Online: false}
		}
	case PresenceOnline:
		previous, known := t.peers[name]
		peer := previous
		peer.Name = name
		if !known {
			peer.Since = time.Now()
		}
		if shape := shapeOf(packet); shape != "" {
			peer.Shape = shape
		}
		if !known || peer.Shape != previous.Shape {
			changed = true
		}
		t.peers[name] = peer
		event = PresenceEvent{Peer: name, Online: true, Shape: peer.Shape}
	}
	t.mu.Unlock()

	if changed && t.options.OnPresence != nil {
		t.options.OnPresence(event)
	}
}

// shapeOf reads the description hash riding beside a retained `online`.
func shapeOf(packet *paho.Publish) string {
	if packet.Properties == nil {
		return ""
	}
	for _, property := range packet.Properties.User {
		if property.Key != shapeProperty {
			continue
		}
		if property.Value == "" || len(property.Value) > maxShapeLength {
			return ""
		}
		return property.Value
	}
	return ""
}

func (t *Transport) armPresenceSweep() {
	if t.presenceHasSettled() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sweepTimer != nil {
		t.sweepTimer.Stop()
	}
	t.sweepTimer = time.AfterFunc(presenceQuietPeriod, t.settlePresence)
}

func (t *Transport) settlePresence() {
	t.mu.Lock()
	if t.sweepTimer != nil {
		t.sweepTimer.Stop()
		t.sweepTimer = nil
	}
	t.mu.Unlock()
	t.settledOnce.Do(func() { close(t.settled) })
}

func (t *Transport) presenceHasSettled() bool {
	select {
	case <-t.settled:
		return true
	default:
		return false
	}
}
