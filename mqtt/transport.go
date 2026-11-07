package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/daritas/rpc-go"
)

const defaultRequestExpirySeconds uint32 = 30

// subscribeTimeout bounds one pass of restoring subscriptions after a connection comes up. It is
// not the caller's deadline: a reconnection happens with nobody waiting on it.
const subscribeTimeout = 30 * time.Second

type TransportOptions struct {
	URL                  string
	Name                 string
	Prefix               string
	QoS                  byte
	Codec                rpc.Codec
	ConnectTimeout       time.Duration
	RequestExpirySeconds uint32
	SessionExpirySeconds uint32
	PersistentSession    bool
	PublishPresence      bool
	SubscribePresence    bool
	Username             string
	Password             string

	// TLS configures the encrypted link, and applies to an mqtts:// broker URL. Left nil against an
	// mqtts:// URL, the broker is verified against the host's trust store under the name in the URL.
	TLS *TLSOptions

	Signer           MessageSigner
	Verifier         MessageVerifier
	MaxClockSkew     time.Duration
	MaxTrackedNonces int
	OnRejected       func(RejectedFrame)

	// AllowResponseTopic replaces the default rule that a request's response topic must sit under
	// this transport's prefix. An installation that answers across prefixes supplies its own.
	AllowResponseTopic func(topic string, source string) bool

	// OnConnection reports the broker connection coming up and going down, including which
	// subscriptions failed to restore. A peer that reconnects into a half-working state should be
	// able to say so rather than going quiet.
	OnConnection func(ConnectionEvent)

	// OnPresence reports a peer arriving or leaving, for a caller that wants the changes rather
	// than to poll Peers(). Needs SubscribePresence.
	OnPresence func(PresenceEvent)

	// Shape is this peer's description hash, announced beside its retained presence so a cache
	// holding a describe() answer can tell a restart that changed the surface from one that did not.
	Shape string

	// ReconnectBackoff decides how long to wait after failed connection attempt N. Defaults to an
	// exponential backoff between one second and a minute.
	ReconnectBackoff func(attempt int) time.Duration
}

type RejectedFrame struct {
	Source string
	Topic  string
	Reason string
	Err    error
}

// ConnectionEvent describes a change in the broker connection.
type ConnectionEvent struct {
	// Up is true when a connection was established, false when one was lost.
	Up bool
	// First is true for the initial connection and false for every reconnection after it.
	First bool
	// FailedSubscriptions names the topics the broker refused on reconnect. A non-empty list means
	// this peer is connected but not serving everything it was serving before.
	FailedSubscriptions []string
	// Err carries why a connection dropped, or why restoring subscriptions failed.
	Err error
}

type Transport struct {
	options TransportOptions
	manager *autopaho.ConnectionManager
	codec   rpc.Codec

	server *rpc.Server

	mu                  sync.Mutex
	pending             map[string]chan response
	events              map[string]map[uint64]rpc.EventHandler
	serverSubscriptions map[string]map[string]struct{}
	peerIdentities      map[string]*rpc.Identity
	replayGuard         *ReplayGuard
	nextSubscriptionID  uint64
	closed              bool
	connected           bool
	lastConnectErr      error

	// ready is closed once the first connection has finished subscribing, which is what Connect
	// waits for. Reconnections after that are the connection manager's business, not the caller's.
	ready     chan struct{}
	readyOnce sync.Once

	// refused is closed when the broker denies this peer's identity, which no amount of retrying
	// will fix and which Connect should therefore report without waiting out its window.
	refused     chan struct{}
	refusedOnce sync.Once

	// peers is what presence has reported, and settled closes once the retained burst that fills it
	// has been read. sweepTimer measures the quiet period that decides when that is.
	peers       map[string]Peer
	settled     chan struct{}
	settledOnce sync.Once
	sweepTimer  *time.Timer
}

type response struct {
	result any
	err    error
}

func NewTransport(options TransportOptions) *Transport {
	if options.Prefix == "" {
		options.Prefix = DefaultPrefix
	}
	if options.QoS == 0 {
		options.QoS = 1
	}
	if options.Codec == nil {
		options.Codec = rpc.MsgPackCodec{}
	}
	if options.ConnectTimeout == 0 {
		options.ConnectTimeout = 5 * time.Second
	}
	if options.RequestExpirySeconds == 0 {
		options.RequestExpirySeconds = defaultRequestExpirySeconds
	}
	return &Transport{
		options:             options,
		codec:               options.Codec,
		pending:             map[string]chan response{},
		events:              map[string]map[uint64]rpc.EventHandler{},
		serverSubscriptions: map[string]map[string]struct{}{},
		peerIdentities:      map[string]*rpc.Identity{},
		replayGuard:         NewReplayGuard(options.MaxClockSkew, options.MaxTrackedNonces),
		ready:               make(chan struct{}),
		refused:             make(chan struct{}),
		peers:               map[string]Peer{},
		settled:             make(chan struct{}),
	}
}

func (t *Transport) Name() string {
	return t.options.Name
}

func (t *Transport) Identity(source string) (*rpc.Identity, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	identity, ok := t.peerIdentities[source]
	return identity, ok
}

// Connect establishes the broker connection and waits for the first subscription pass to finish.
//
// The connection is managed from here on: a dropped link is redialled with backoff and every
// subscription is restored, without the caller doing anything. Connect still reports an initial
// failure — an unreachable broker or refused credentials — because a peer that cannot start is a
// configuration problem for whoever started it, not something to retry over silently.
func (t *Transport) Connect(ctx context.Context) error {
	if t.options.Name == "" {
		return errors.New("mqtt transport: Name is required")
	}
	serverURL, err := brokerURL(t.options.URL)
	if err != nil {
		return err
	}

	config := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{serverURL},
		KeepAlive:                     30,
		CleanStartOnInitialConnection: !t.options.PersistentSession,
		ConnectTimeout:                t.options.ConnectTimeout,
		ReconnectBackoff:              t.reconnectBackoff(),
		ConnectUsername:               t.options.Username,
		OnConnectionUp:                t.onConnectionUp,
		OnConnectionDown:              t.onConnectionDown,
		OnConnectError:                t.onConnectError,
		ClientConfig: paho.ClientConfig{
			ClientID: t.options.Name,
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				t.onPublish,
			},
		},
	}
	if isTLSScheme(serverURL.Scheme) {
		tlsConfig, err := t.options.TLS.config(serverURL.Hostname())
		if err != nil {
			return err
		}
		config.TlsCfg = tlsConfig
	} else if t.options.TLS != nil {
		// Refused rather than ignored. Silently dropping a TLS configuration would leave the caller
		// believing the link is encrypted while the credentials below go over the wire in the clear.
		return fmt.Errorf("mqtt transport: TLS options were given for the plain %q URL %q; use mqtts://", serverURL.Scheme, t.options.URL)
	}
	if t.options.Password != "" {
		config.ConnectPassword = []byte(t.options.Password)
	}
	if t.options.PersistentSession || t.options.SessionExpirySeconds > 0 {
		config.SessionExpiryInterval = t.options.SessionExpirySeconds
		if config.SessionExpiryInterval == 0 {
			config.SessionExpiryInterval = 3600
		}
	}
	if t.options.PublishPresence {
		config.WillMessage = &paho.WillMessage{
			Topic:   PresenceTopic(t.options.Prefix, t.options.Name),
			Payload: []byte(PresenceOffline),
			Retain:  true,
			QoS:     t.options.QoS,
		}
	}

	// Detached from the caller's context on purpose: ctx bounds how long we wait for the *first*
	// connection, while the manager has to outlive it and keep reconnecting for the process's life.
	manager, err := autopaho.NewConnection(context.WithoutCancel(ctx), config)
	if err != nil {
		return fmt.Errorf("mqtt transport: %w", err)
	}
	t.mu.Lock()
	t.manager = manager
	t.mu.Unlock()

	waitCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && t.options.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, t.options.ConnectTimeout)
		defer cancel()
	}
	connected := make(chan error, 1)
	go func() { connected <- manager.AwaitConnection(waitCtx) }()
	select {
	case err := <-connected:
		if err != nil {
			return t.abandonConnection(manager, err)
		}
	case <-t.refused:
		return t.abandonConnection(manager, errors.New("broker denied the connection"))
	case <-waitCtx.Done():
		return t.abandonConnection(manager, waitCtx.Err())
	}
	select {
	case <-t.ready:
		return nil
	case <-waitCtx.Done():
		return t.abandonConnection(manager, waitCtx.Err())
	}
}

// abandonConnection tears down a manager whose first connection never completed, so a failed
// Connect does not leave a goroutine retrying credentials the caller has already given up on.
func (t *Transport) abandonConnection(manager *autopaho.ConnectionManager, err error) error {
	_ = manager.Disconnect(context.Background())
	t.mu.Lock()
	t.manager = nil
	connectErr := t.lastConnectErr
	t.mu.Unlock()
	if connectErr != nil {
		return fmt.Errorf("mqtt transport: %w", connectErr)
	}
	return fmt.Errorf("mqtt transport: %w", err)
}

func (t *Transport) reconnectBackoff() func(int) time.Duration {
	if t.options.ReconnectBackoff != nil {
		return t.options.ReconnectBackoff
	}
	return autopaho.NewExponentialBackoff(time.Second, time.Minute, 2*time.Second, 2)
}

// onConnectionUp restores everything this peer was serving before the link dropped.
//
// A reconnect that quietly comes back subscribed to less than it was is the failure mode worth
// guarding against: the peer looks healthy, answers presence, and silently serves nothing on the
// topics the broker refused. So the refusals are collected and reported rather than discarded.
func (t *Transport) onConnectionUp(manager *autopaho.ConnectionManager, _ *paho.Connack) {
	// Must not block the connection manager, so the subscribe pass runs on its own goroutine.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
		defer cancel()

		t.mu.Lock()
		first := !t.connected
		t.connected = true
		t.lastConnectErr = nil
		t.mu.Unlock()

		failed, err := t.subscribeAll(ctx, manager)
		if err == nil {
			// The retained burst starts arriving now the subscription is granted, so this is where
			// the quiet period that decides it has finished first arms.
			if t.options.SubscribePresence {
				t.armPresenceSweep()
			} else {
				t.settlePresence()
			}
		}
		if err == nil && t.options.PublishPresence {
			err = t.publishPresence(ctx, PresenceOnline)
		}
		if err == nil {
			t.readyOnce.Do(func() { close(t.ready) })
		}
		t.reportConnection(ConnectionEvent{Up: true, First: first, FailedSubscriptions: failed, Err: err})
	}()
}

// onConnectionDown fails the calls that were in flight when the link dropped.
//
// Failing fast rather than holding them for the reconnection: the reply was to be published to a
// topic this peer is no longer subscribed to, so waiting would usually mean waiting out the
// caller's timeout to reach the same answer, only later.
func (t *Transport) onConnectionDown() bool {
	t.mu.Lock()
	t.connected = false
	closed := t.closed
	t.mu.Unlock()

	t.failPending(&rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: "broker connection lost"}})
	t.reportConnection(ConnectionEvent{Up: false})
	// False stops the manager reconnecting, which is only what we want once Close has been called.
	return !closed
}

func (t *Transport) onConnectError(err error) {
	t.mu.Lock()
	t.lastConnectErr = err
	t.mu.Unlock()
	if isRefusedIdentity(err) {
		t.refusedOnce.Do(func() { close(t.refused) })
	}
}

// isRefusedIdentity reports whether the broker denied this peer for who it says it is, rather than
// failing to answer.
//
// The distinction decides whether retrying is worth anything. A dial that failed or a broker that is
// still starting will succeed on a later attempt, which is the whole point of the backoff. A broker
// that says the credentials or the client id are wrong will say so again every ten seconds until a
// human changes something, so Connect reports it immediately instead of waiting out its window.
func isRefusedIdentity(err error) bool {
	var connack *autopaho.ConnackError
	if !errors.As(err, &connack) {
		return false
	}
	switch connack.ReasonCode {
	case 0x85, // Client Identifier not valid
		0x86, // Bad User Name or Password
		0x87, // Not authorized
		0x8A, // Banned
		0x99: // Payload format invalid
		return true
	default:
		return false
	}
}

func (t *Transport) reportConnection(event ConnectionEvent) {
	if t.options.OnConnection != nil {
		t.options.OnConnection(event)
	}
}

func (t *Transport) AttachServer(server *rpc.Server) {
	t.server = server
}

func (t *Transport) Close(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	manager := t.manager
	t.mu.Unlock()

	var result error
	if manager != nil {
		if t.options.PublishPresence {
			if err := t.publishPresence(ctx, PresenceOffline); err != nil {
				result = err
			}
		}
		if err := manager.Disconnect(ctx); err != nil && result == nil {
			result = err
		}
	}
	t.failPending(&rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: "transport closed"}})
	return result
}

func (t *Transport) Announce(ctx context.Context) error {
	return t.publishPresence(ctx, PresenceOnline)
}

func (t *Transport) Call(ctx context.Context, source string, target string, call rpc.CallPayload) (any, error) {
	if t.connectionManager() == nil {
		return nil, &rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: "transport is not connected"}}
	}
	frame, ok := ToFrame(rpc.Message{Type: rpc.MessageTypeRequest, Payload: call})
	if !ok {
		return nil, &rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: "call cannot be represented as an MQTT frame"}}
	}
	wait := make(chan response, 1)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, &rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: "transport closed"}}
	}
	t.pending[call.ID] = wait
	t.mu.Unlock()
	defer t.forget(call.ID)

	if err := t.publishFrame(ctx, source, target, frame, t.codec.ContentType()); err != nil {
		return nil, &rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: err.Error()}}
	}
	select {
	case reply := <-wait:
		return reply.result, reply.err
	case <-ctx.Done():
		return nil, &rpc.RPCError{Code: rpc.ErrorTimeout, Err: &rpc.RemoteError{Name: "RpcError", Message: ctx.Err().Error()}}
	}
}

func (t *Transport) Subscribe(ctx context.Context, source string, target string, path string, event string, handler rpc.EventHandler) (func(context.Context) error, error) {
	if handler == nil {
		return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "event handler is nil"}}
	}
	key := clientEventKey(target, path, event)
	t.mu.Lock()
	t.nextSubscriptionID++
	id := t.nextSubscriptionID
	if t.events[key] == nil {
		t.events[key] = map[uint64]rpc.EventHandler{}
	}
	t.events[key][id] = handler
	t.mu.Unlock()

	call := rpc.CallPayload{
		ID:     rpc.NewID(),
		Type:   rpc.RPCMessageCall,
		Path:   path,
		Method: "on",
		Params: []any{event},
	}
	if _, err := t.Call(ctx, source, target, call); err != nil {
		t.removeEventHandler(key, id)
		return nil, err
	}
	return func(unsubscribeCtx context.Context) error {
		remaining := t.removeEventHandler(key, id)
		if remaining {
			return nil
		}
		off := rpc.CallPayload{
			ID:     rpc.NewID(),
			Type:   rpc.RPCMessageCall,
			Path:   path,
			Method: "off",
			Params: []any{event},
		}
		_, err := t.Call(unsubscribeCtx, source, target, off)
		return err
	}, nil
}

func (t *Transport) PublishEvent(ctx context.Context, path string, event string, params []any) error {
	key := serverEventKey(path, event)
	t.mu.Lock()
	targets := make([]string, 0, len(t.serverSubscriptions[key]))
	for target := range t.serverSubscriptions[key] {
		targets = append(targets, target)
	}
	t.mu.Unlock()
	for _, target := range targets {
		frame, ok := ToFrame(rpc.Message{
			Type: rpc.MessageTypeEvent,
			Payload: rpc.EventPayload{
				Type:   rpc.RPCMessageEvent,
				Event:  event,
				Path:   path,
				Params: params,
			},
		})
		if !ok {
			return fmt.Errorf("event cannot be represented as an MQTT frame")
		}
		if err := t.publishFrame(ctx, t.options.Name, target, frame, t.codec.ContentType()); err != nil {
			return err
		}
	}
	return nil
}

// subscriptionTopics is everything this peer needs the broker to route to it. The same list is used
// for the first connection and every restoration after a drop, so the two cannot drift apart.
func (t *Transport) subscriptionTopics() []string {
	topics := []string{
		Topic(t.options.Prefix, ChannelResponse, t.options.Name),
		Topic(t.options.Prefix, ChannelEvent, t.options.Name),
		Topic(t.options.Prefix, ChannelRequest, t.options.Name),
	}
	if t.options.SubscribePresence {
		topics = append(topics, t.options.Prefix+"/presence/+")
	}
	return topics
}

// subscribeAll subscribes every topic and names the ones the broker refused.
//
// A SUBACK carries a reason code per topic, and a refusal there is not an error on the call: the
// subscribe succeeds while leaving this peer deaf on that topic. Reading the codes is the only way
// to tell the difference.
func (t *Transport) subscribeAll(ctx context.Context, manager *autopaho.ConnectionManager) ([]string, error) {
	topics := t.subscriptionTopics()
	subscriptions := make([]paho.SubscribeOptions, 0, len(topics))
	for _, topic := range topics {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: topic, QoS: t.options.QoS})
	}
	suback, err := manager.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil {
		return topics, err
	}
	return refusedSubscriptions(topics, suback), nil
}

// refusedSubscriptions names the topics whose SUBACK reason code is a failure. Reason codes at or
// above 0x80 are refusals; anything below is the granted QoS.
func refusedSubscriptions(topics []string, suback *paho.Suback) []string {
	if suback == nil {
		return nil
	}
	var refused []string
	for index, topic := range topics {
		if index >= len(suback.Reasons) {
			refused = append(refused, topic)
			continue
		}
		if suback.Reasons[index] >= 0x80 {
			refused = append(refused, topic)
		}
	}
	return refused
}

func (t *Transport) onPublish(pr paho.PublishReceived) (bool, error) {
	packet := pr.Packet
	if packet == nil {
		return false, nil
	}
	// Stamped before any work, because it is what the caller's budget is measured from. Anything
	// this peer spends decoding and verifying is time the caller is already waiting.
	arrived := time.Now()
	if strings.HasPrefix(packet.Topic, t.options.Prefix+"/presence/") {
		t.handlePresence(packet)
		return true, nil
	}
	frame, source, contentType, err := t.decodePublish(packet)
	if err != nil {
		t.reject(packet, sourceFromPublish(packet), err.Error(), err)
		return true, nil
	}
	message, ok := FromFrame(frame)
	if !ok {
		t.reject(packet, source, "invalid frame", nil)
		return true, nil
	}
	switch frame.Channel {
	case ChannelResponse:
		t.deliverResponse(frame.Correlation, message)
	case ChannelRequest:
		if t.server != nil {
			responseTopic, reason := t.resolveResponseTopic(responseTopicOf(packet), source)
			if reason != "" {
				t.reject(packet, source, reason, nil)
				return true, nil
			}
			go t.answerRequest(source, packet, responseTopic, frame, message, arrived)
		}
	case ChannelEvent:
		t.deliverEvent(source, message)
	}
	_ = contentType
	return true, nil
}

func (t *Transport) answerRequest(source string, packet *paho.Publish, responseTopic string, frame Frame, message rpc.Message, arrived time.Time) {
	call, ok := message.Payload.(rpc.CallPayload)
	if !ok {
		return
	}
	identity, _ := t.Identity(source)
	if frame.Kind == KindSubscribe || frame.Kind == KindUnsubscribe {
		t.answerSubscription(source, identity, packet, responseTopic, frame, call)
		return
	}
	// The caller's budget bounds the method as well as deciding whether to run it at all: a handler
	// left running past the point its caller gave up holds a reply nobody is waiting for.
	ctx := context.Background()
	if call.TTL > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, arrived.Add(time.Duration(call.TTL)*time.Millisecond))
		defer cancel()
	}
	reply := t.server.DispatchArrivedWithIdentity(ctx, source, identity, call, arrived)
	t.answer(packet, responseTopic, frame, reply)
}

func (t *Transport) answerSubscription(source string, identity *rpc.Identity, packet *paho.Publish, responseTopic string, frame Frame, call rpc.CallPayload) {
	if len(call.Params) < 1 {
		t.answer(packet, responseTopic, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorInvalidParams, call.Path+"."+call.Method+" expects an event name"))
		return
	}
	event, ok := call.Params[0].(string)
	if !ok || event == "" {
		t.answer(packet, responseTopic, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorInvalidParams, call.Path+"."+call.Method+" expects an event name"))
		return
	}
	if t.server != nil && !t.server.HasEvent(call.Path, event) {
		t.answer(packet, responseTopic, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorMethodNotFound, call.Path+"."+event+" is not a described event"))
		return
	}
	if t.server != nil {
		callContext := rpc.CallContext{
			Source:       source,
			Identity:     identity,
			InstanceName: call.Path,
			Method:       event,
			Params:       call.Params,
			Subscription: true,
		}
		requiresAuthority := t.server.EventRequiresAuthority(call.Path, event)
		if err := t.server.CheckAuthorization(context.Background(), callContext, requiresAuthority); err != nil {
			t.answer(packet, responseTopic, frame, rpc.ErrorPayload{ID: call.ID, Type: rpc.RPCMessageError, Code: err.Code, Error: err.Err})
			return
		}
	}
	key := serverEventKey(call.Path, event)
	t.mu.Lock()
	if frame.Kind == KindSubscribe {
		if t.serverSubscriptions[key] == nil {
			t.serverSubscriptions[key] = map[string]struct{}{}
		}
		t.serverSubscriptions[key][source] = struct{}{}
	} else {
		delete(t.serverSubscriptions[key], source)
		if len(t.serverSubscriptions[key]) == 0 {
			delete(t.serverSubscriptions, key)
		}
	}
	t.mu.Unlock()
	t.answer(packet, responseTopic, frame, rpc.NewSuccessPayload(call.ID, "ok"))
}

// answer publishes a reply on the topic resolveResponseTopic already settled on. The topic is
// decided once, on the receive path, so that a request naming a hostile one is refused before any
// method runs rather than having its answer redirected afterwards.
func (t *Transport) answer(packet *paho.Publish, responseTopic string, frame Frame, reply any) {
	if reply == nil {
		return
	}
	replyFrame, ok := ToFrame(rpc.Message{Payload: reply})
	if !ok {
		return
	}
	contentType := t.codec.ContentType()
	if packet.Properties != nil && packet.Properties.ContentType != "" {
		contentType = packet.Properties.ContentType
	}
	replyFrame.Correlation = frame.Correlation
	if err := t.publishFrameToTopic(context.Background(), t.options.Name, responseTopic, replyFrame, contentType); err != nil {
		return
	}
}

func (t *Transport) deliverResponse(correlation string, message rpc.Message) {
	t.mu.Lock()
	wait := t.pending[correlation]
	t.mu.Unlock()
	if wait == nil {
		return
	}
	switch payload := message.Payload.(type) {
	case rpc.SuccessPayload:
		wait <- response{result: payload.Result}
	case rpc.ErrorPayload:
		wait <- response{err: &rpc.RPCError{Code: payload.Code, Err: payload.Error}}
	}
}

func (t *Transport) deliverEvent(source string, message rpc.Message) {
	payload, ok := message.Payload.(rpc.EventPayload)
	if !ok {
		return
	}
	key := clientEventKey(source, payload.Path, payload.Event)
	t.mu.Lock()
	handlers := make([]rpc.EventHandler, 0, len(t.events[key]))
	for _, handler := range t.events[key] {
		handlers = append(handlers, handler)
	}
	t.mu.Unlock()
	for _, handler := range handlers {
		handler(payload)
	}
}

func (t *Transport) publishFrame(ctx context.Context, source string, target string, frame Frame, contentType string) error {
	return t.publishFrameToTopic(ctx, source, Topic(t.options.Prefix, frame.Channel, target), frame, contentType)
}

func (t *Transport) publishFrameToTopic(ctx context.Context, source string, topic string, frame Frame, contentType string) error {
	manager := t.connectionManager()
	if manager == nil {
		return errors.New("mqtt transport: not connected")
	}
	body, err := t.encodeBody(contentType, frame.Body)
	if err != nil {
		return err
	}
	properties := &paho.PublishProperties{
		ContentType: contentType,
	}
	control := frame.ControlProperties(source)
	properties.PayloadFormat = payloadFormat(contentType)
	if frame.Correlation != "" {
		properties.CorrelationData = []byte(frame.Correlation)
	}
	responseTopic := ""
	if IsRequestKind(frame.Kind) {
		responseTopic = Topic(t.options.Prefix, ChannelResponse, source)
		properties.ResponseTopic = responseTopic
		expiry := requestExpiry(frame.TTL, t.options.RequestExpirySeconds)
		properties.MessageExpiry = &expiry
	}
	if t.options.Signer != nil {
		nonce, err := createNonce()
		if err != nil {
			return err
		}
		timestamp := time.Now().UnixMilli()
		canonical, err := CanonicalSignedBytesV5(SignedFrameV5{
			Version:         FrameVersion,
			Topic:           topic,
			ResponseTopic:   responseTopic,
			Source:          source,
			Kind:            string(frame.Kind),
			Path:            frame.Path,
			MethodOrEvent:   methodOrEvent(frame),
			Correlation:     frame.Correlation,
			ContentType:     contentType,
			Code:            string(frame.Code),
			ContractVersion: frame.Version,
			TTL:             control[MRTTL],
			IdempotencyKey:  frame.IdempotencyKey,
			Timestamp:       timestamp,
			Nonce:           nonce,
			Payload:         body,
		})
		if err != nil {
			return err
		}
		signature, err := t.options.Signer.Sign(ctx, canonical, SigningContext{Source: source})
		if err != nil {
			return err
		}
		control[MRNonce] = nonce
		control[MRTimestamp] = strconv.FormatInt(timestamp, 10)
		control[MRSignature] = signature
	}
	properties.User = userProperties(control)
	_, err = manager.Publish(ctx, &paho.Publish{
		Topic:      topic,
		QoS:        t.options.QoS,
		Properties: properties,
		Payload:    body,
	})
	return err
}

func (t *Transport) decodePublish(packet *paho.Publish) (Frame, string, string, error) {
	if packet.Properties == nil {
		return Frame{}, "", "", errors.New("missing properties")
	}
	properties, err := controlProperties(packet.Properties.User)
	if err != nil {
		return Frame{}, "", "", err
	}
	source := properties[MRSource]
	// Checked once, here, because everything downstream trusts it: the source names a topic this
	// peer publishes to and keys the subscription and identity maps. A name carrying `#` or a NUL
	// would reach all of those.
	if source != "" && !isSafeTopicSegment(source) {
		return Frame{}, "", "", fmt.Errorf("unusable %s", MRSource)
	}
	contentType := packet.Properties.ContentType
	if contentType != "" && !t.supportedContentType(contentType) {
		return Frame{}, source, "", fmt.Errorf("unsupported content type %q", contentType)
	}
	if t.options.Verifier != nil {
		if err := t.verifyPublish(context.Background(), packet, properties); err != nil {
			return Frame{}, source, "", err
		}
	}
	if contentType == "" {
		contentType = t.codec.ContentType()
	}
	body, err := t.decodeBody(contentType, packet.Payload)
	if err != nil {
		return Frame{}, source, "", err
	}
	frame, ok := FrameFromControlProperties(properties, body, string(packet.Properties.CorrelationData))
	if !ok {
		return Frame{}, source, "", errors.New("invalid control properties")
	}
	// After verification, never before: the signature covers the ttl the caller stated, and what the
	// broker has left of it is not part of what was signed.
	frame.TTL = remainingTTL(frame.TTL, packet.Properties.MessageExpiry)
	return frame, source, contentType, nil
}

func (t *Transport) verifyPublish(ctx context.Context, packet *paho.Publish, properties map[string]string) error {
	source := properties[MRSource]
	signature := properties[MRSignature]
	nonce := properties[MRNonce]
	timestamp, err := strconv.ParseInt(properties[MRTimestamp], 10, 64)
	if signature == "" || nonce == "" || err != nil {
		return errors.New("unsigned")
	}
	version := properties[MRVersion]
	if version == "" {
		version = FrameVersion
	}
	if version != FrameVersion {
		return fmt.Errorf("signed frame version %q is not supported", version)
	}
	if !t.replayGuard.Accept(nonce, timestamp) {
		return errors.New("stale or replayed")
	}
	responseTopic := ""
	if IsRequestKind(Kind(properties[MRKind])) {
		responseTopic = packet.Properties.ResponseTopic
	}
	correlation := string(packet.Properties.CorrelationData)
	canonical, err := CanonicalSignedBytesV5(SignedFrameV5{
		Version:         version,
		Topic:           packet.Topic,
		ResponseTopic:   responseTopic,
		Source:          source,
		Kind:            properties[MRKind],
		Path:            properties[MRPath],
		MethodOrEvent:   methodOrEventProperties(properties),
		Correlation:     correlation,
		ContentType:     packet.Properties.ContentType,
		Code:            properties[MRCode],
		ContractVersion: properties[MRContractVersion],
		TTL:             properties[MRTTL],
		IdempotencyKey:  properties[MRIdempotencyKey],
		Timestamp:       timestamp,
		Nonce:           nonce,
		Payload:         packet.Payload,
	})
	if err != nil {
		return err
	}
	identity, err := t.options.Verifier.Verify(ctx, canonical, signature, SigningContext{Source: source})
	if err != nil {
		return fmt.Errorf("verifier error: %w", err)
	}
	if identity == nil {
		return errors.New("bad signature")
	}
	if identity.Name != source {
		return errors.New("identity does not match source")
	}
	t.mu.Lock()
	t.peerIdentities[source] = identity
	t.mu.Unlock()
	return nil
}

func (t *Transport) decodeBody(contentType string, payload []byte) (any, error) {
	if contentType == "" || contentType == t.codec.ContentType() {
		return t.codec.DecodeValue(payload)
	}
	if contentType == (rpc.JSONCodec{}).ContentType() {
		return (rpc.JSONCodec{}).DecodeValue(payload)
	}
	if contentType == (rpc.MsgPackCodec{}).ContentType() {
		return (rpc.MsgPackCodec{}).DecodeValue(payload)
	}
	return nil, fmt.Errorf("unsupported content type %q", contentType)
}

func (t *Transport) supportedContentType(contentType string) bool {
	return contentType == "" ||
		contentType == t.codec.ContentType() ||
		contentType == (rpc.JSONCodec{}).ContentType() ||
		contentType == (rpc.MsgPackCodec{}).ContentType()
}

func (t *Transport) encodeBody(contentType string, body any) ([]byte, error) {
	if contentType == "" || contentType == t.codec.ContentType() {
		return t.codec.Encode(body)
	}
	if contentType == (rpc.JSONCodec{}).ContentType() {
		return (rpc.JSONCodec{}).Encode(body)
	}
	if contentType == (rpc.MsgPackCodec{}).ContentType() {
		return (rpc.MsgPackCodec{}).Encode(body)
	}
	return nil, fmt.Errorf("unsupported content type %q", contentType)
}

func (t *Transport) connectionManager() *autopaho.ConnectionManager {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.manager
}

func (t *Transport) publishPresence(ctx context.Context, state string) error {
	manager := t.connectionManager()
	if manager == nil {
		return errors.New("mqtt transport: not connected")
	}
	publish := &paho.Publish{
		Topic:   PresenceTopic(t.options.Prefix, t.options.Name),
		QoS:     t.options.QoS,
		Retain:  true,
		Payload: []byte(state),
	}
	// The shape rides beside the payload, never inside it: the payload has to stay the exact string
	// every already-deployed peer compares against.
	if state == PresenceOnline && t.options.Shape != "" {
		publish.Properties = &paho.PublishProperties{
			User: userProperties(map[string]string{shapeProperty: t.options.Shape}),
		}
	}
	_, err := manager.Publish(ctx, publish)
	return err
}

func (t *Transport) forget(correlation string) {
	t.mu.Lock()
	delete(t.pending, correlation)
	t.mu.Unlock()
}

func (t *Transport) removeEventHandler(key string, id uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.events[key], id)
	if len(t.events[key]) == 0 {
		delete(t.events, key)
		return false
	}
	return true
}

func (t *Transport) failPending(err error) {
	t.mu.Lock()
	pending := t.pending
	t.pending = map[string]chan response{}
	t.mu.Unlock()
	for _, wait := range pending {
		wait <- response{err: err}
	}
}

func (t *Transport) reject(packet *paho.Publish, source string, reason string, err error) {
	if t.options.OnRejected == nil {
		return
	}
	topic := ""
	if packet != nil {
		topic = packet.Topic
	}
	t.options.OnRejected(RejectedFrame{
		Source: source,
		Topic:  topic,
		Reason: reason,
		Err:    err,
	})
}

func payloadFormat(contentType string) *byte {
	var format byte
	switch contentType {
	case (rpc.JSONCodec{}).ContentType():
		format = 1
	case (rpc.MsgPackCodec{}).ContentType(), "":
		format = 0
	default:
		return nil
	}
	return &format
}

func clientEventKey(source string, path string, event string) string {
	return source + "\x00" + path + "\x00" + event
}

func serverEventKey(path string, event string) string {
	return path + "\x00" + event
}

// remainingTTL is what is left of the caller's stated budget.
//
// MQTT 5 requires a broker to hand on the Message Expiry Interval it received minus the time the
// message spent queued in it, so for a request that waited this is the caller's own budget with the
// queueing already deducted — measured by the broker, with neither peer's clock entering into it.
// Taken as an upper bound only: expiry is whole seconds and a broker of unknown quality set it, so
// it may shorten what the caller signed and never lengthen it.
func remainingTTL(stated int64, expirySeconds *uint32) int64 {
	if stated <= 0 || expirySeconds == nil {
		return stated
	}
	if remaining := int64(*expirySeconds) * 1000; remaining < stated {
		return remaining
	}
	return stated
}

func responseTopicOf(packet *paho.Publish) string {
	if packet == nil || packet.Properties == nil {
		return ""
	}
	return packet.Properties.ResponseTopic
}

func requestExpiry(ttl int64, fallback uint32) uint32 {
	if ttl <= 0 {
		return fallback
	}
	seconds := (ttl + 999) / 1000
	if seconds <= 0 {
		return 1
	}
	return uint32(seconds)
}

func userProperties(values map[string]string) paho.UserProperties {
	var properties paho.UserProperties
	for key, value := range values {
		properties.Add(key, value)
	}
	return properties
}

func controlProperties(properties paho.UserProperties) (map[string]string, error) {
	values := map[string]string{}
	for _, property := range properties {
		if !strings.HasPrefix(property.Key, "mr-") {
			continue
		}
		if _, exists := values[property.Key]; exists {
			return nil, fmt.Errorf("duplicate control property %s", property.Key)
		}
		values[property.Key] = property.Value
	}
	return values, nil
}

func sourceFromPublish(packet *paho.Publish) string {
	if packet == nil || packet.Properties == nil {
		return ""
	}
	for _, property := range packet.Properties.User {
		if property.Key == MRSource {
			return property.Value
		}
	}
	return ""
}

// brokerURL normalises the broker address into the form the connection manager dials, filling in
// the port that goes with the scheme so a URL without one does not depend on the library's default.
//
// WebSocket schemes are refused rather than passed through: this implementation is MQTT-first by
// design, and a ws:// peer belongs on the TypeScript side.
func brokerURL(raw string) (*url.URL, error) {
	if raw == "" {
		raw = "mqtt://localhost:" + defaultPlainPort
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	switch {
	case isPlainScheme(parsed.Scheme), isTLSScheme(parsed.Scheme):
	default:
		return nil, fmt.Errorf("mqtt transport: unsupported broker scheme %q", parsed.Scheme)
	}
	if parsed.Hostname() == "" {
		return nil, fmt.Errorf("mqtt transport: broker URL %q has no host", raw)
	}
	if parsed.Port() == "" {
		port := defaultPlainPort
		if isTLSScheme(parsed.Scheme) {
			port = defaultTLSPort
		}
		parsed.Host = net.JoinHostPort(parsed.Hostname(), port)
	}
	return parsed, nil
}
