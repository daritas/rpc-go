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

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	rpc "github.com/source-repo/rpc-go"
)

const defaultRequestExpirySeconds uint32 = 30

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
	Signer               MessageSigner
	Verifier             MessageVerifier
	MaxClockSkew         time.Duration
	MaxTrackedNonces     int
}

type Transport struct {
	options TransportOptions
	client  *paho.Client
	conn    net.Conn
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

func (t *Transport) Connect(ctx context.Context) error {
	if t.options.Name == "" {
		return errors.New("mqtt transport: Name is required")
	}
	address, err := brokerAddress(t.options.URL)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{}
	dialCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline && t.options.ConnectTimeout > 0 {
		dialCtx, cancel = context.WithTimeout(ctx, t.options.ConnectTimeout)
		defer cancel()
	}
	conn, err := dialer.DialContext(dialCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("mqtt transport: dial %s: %w", address, err)
	}
	t.conn = conn
	t.client = paho.NewClient(paho.ClientConfig{
		ClientID: t.options.Name,
		Conn:     packets.NewThreadSafeConn(conn),
		OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			t.onPublish,
		},
		OnClientError: func(err error) {
			t.failPending(&rpc.RPCError{Code: rpc.ErrorTransport, Err: &rpc.RemoteError{Name: "RpcError", Message: err.Error()}})
		},
	})

	cleanStart := !t.options.PersistentSession
	connect := &paho.Connect{
		ClientID:   t.options.Name,
		KeepAlive:  30,
		CleanStart: cleanStart,
	}
	if t.options.Username != "" {
		connect.Username = t.options.Username
		connect.UsernameFlag = true
	}
	if t.options.Password != "" {
		connect.Password = []byte(t.options.Password)
		connect.PasswordFlag = true
	}
	if t.options.PersistentSession || t.options.SessionExpirySeconds > 0 {
		seconds := t.options.SessionExpirySeconds
		if seconds == 0 {
			seconds = 3600
		}
		connect.Properties = &paho.ConnectProperties{SessionExpiryInterval: &seconds}
	}
	if t.options.PublishPresence {
		connect.WillMessage = &paho.WillMessage{
			Topic:   PresenceTopic(t.options.Prefix, t.options.Name),
			Payload: []byte(PresenceOffline),
			Retain:  true,
			QoS:     t.options.QoS,
		}
	}
	connack, err := t.client.Connect(ctx, connect)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if connack.ReasonCode >= 0x80 {
		_ = conn.Close()
		reason := ""
		if connack.Properties != nil {
			reason = connack.Properties.ReasonString
		}
		return fmt.Errorf("mqtt transport: connect refused with reason code %d %s", connack.ReasonCode, reason)
	}
	return t.subscribe(ctx)
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
	t.mu.Unlock()

	var result error
	if t.options.PublishPresence && t.client != nil {
		if err := t.publishPresence(ctx, PresenceOffline); err != nil {
			result = err
		}
	}
	if t.client != nil {
		if err := t.client.Disconnect(&paho.Disconnect{ReasonCode: 0}); err != nil && result == nil {
			result = err
		}
	}
	if t.conn != nil {
		if err := t.conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) && result == nil {
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
	if t.client == nil {
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

func (t *Transport) subscribe(ctx context.Context) error {
	subscriptions := []paho.SubscribeOptions{
		{Topic: Topic(t.options.Prefix, ChannelResponse, t.options.Name), QoS: t.options.QoS},
		{Topic: Topic(t.options.Prefix, ChannelEvent, t.options.Name), QoS: t.options.QoS},
		{Topic: Topic(t.options.Prefix, ChannelRequest, t.options.Name), QoS: t.options.QoS},
	}
	if t.options.SubscribePresence {
		subscriptions = append(subscriptions, paho.SubscribeOptions{Topic: t.options.Prefix + "/presence/+", QoS: t.options.QoS})
	}
	_, err := t.client.Subscribe(ctx, &paho.Subscribe{Subscriptions: subscriptions})
	if err != nil {
		return err
	}
	if t.options.PublishPresence {
		return t.publishPresence(ctx, PresenceOnline)
	}
	return nil
}

func (t *Transport) onPublish(pr paho.PublishReceived) (bool, error) {
	packet := pr.Packet
	if packet == nil {
		return false, nil
	}
	if strings.HasPrefix(packet.Topic, t.options.Prefix+"/presence/") {
		return true, nil
	}
	frame, source, contentType, err := t.decodePublish(packet)
	if err != nil {
		return true, nil
	}
	message, ok := FromFrame(frame)
	if !ok {
		return true, nil
	}
	switch frame.Channel {
	case ChannelResponse:
		t.deliverResponse(frame.Correlation, message)
	case ChannelRequest:
		if t.server != nil {
			go t.answerRequest(source, packet, frame, message)
		}
	case ChannelEvent:
		t.deliverEvent(source, message)
	}
	_ = contentType
	return true, nil
}

func (t *Transport) answerRequest(source string, packet *paho.Publish, frame Frame, message rpc.Message) {
	call, ok := message.Payload.(rpc.CallPayload)
	if !ok {
		return
	}
	if frame.Kind == KindSubscribe || frame.Kind == KindUnsubscribe {
		t.answerSubscription(source, packet, frame, call)
		return
	}
	reply := t.server.Dispatch(context.Background(), source, call)
	t.answer(source, packet, frame, reply)
}

func (t *Transport) answerSubscription(source string, packet *paho.Publish, frame Frame, call rpc.CallPayload) {
	if len(call.Params) < 1 {
		t.answer(source, packet, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorInvalidParams, call.Path+"."+call.Method+" expects an event name"))
		return
	}
	event, ok := call.Params[0].(string)
	if !ok || event == "" {
		t.answer(source, packet, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorInvalidParams, call.Path+"."+call.Method+" expects an event name"))
		return
	}
	if t.server != nil && !t.server.HasEvent(call.Path, event) {
		t.answer(source, packet, frame, rpc.NewErrorPayload(call.ID, rpc.ErrorMethodNotFound, call.Path+"."+event+" is not a described event"))
		return
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
	t.answer(source, packet, frame, rpc.NewSuccessPayload(call.ID, "ok"))
}

func (t *Transport) answer(source string, packet *paho.Publish, frame Frame, reply any) {
	if reply == nil {
		return
	}
	replyFrame, ok := ToFrame(rpc.Message{Payload: reply})
	if !ok {
		return
	}
	responseTopic := ""
	contentType := t.codec.ContentType()
	if packet.Properties != nil {
		responseTopic = packet.Properties.ResponseTopic
		if packet.Properties.ContentType != "" {
			contentType = packet.Properties.ContentType
		}
	}
	if responseTopic == "" {
		responseTopic = Topic(t.options.Prefix, ChannelResponse, source)
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
	_, err = t.client.Publish(ctx, &paho.Publish{
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
	if t.options.Verifier != nil {
		if err := t.verifyPublish(context.Background(), packet, properties); err != nil {
			return Frame{}, "", "", err
		}
	}
	contentType := packet.Properties.ContentType
	if contentType == "" {
		contentType = t.codec.ContentType()
	}
	body, err := t.decodeBody(contentType, packet.Payload)
	if err != nil {
		return Frame{}, "", "", err
	}
	frame, ok := FrameFromControlProperties(properties, body, string(packet.Properties.CorrelationData))
	if !ok {
		return Frame{}, "", "", errors.New("invalid control properties")
	}
	return frame, properties[MRSource], contentType, nil
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

func (t *Transport) publishPresence(ctx context.Context, state string) error {
	_, err := t.client.Publish(ctx, &paho.Publish{
		Topic:   PresenceTopic(t.options.Prefix, t.options.Name),
		QoS:     t.options.QoS,
		Retain:  true,
		Payload: []byte(state),
	})
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

func brokerAddress(raw string) (string, error) {
	if raw == "" {
		raw = "mqtt://localhost:1883"
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "mqtt" {
		return "", fmt.Errorf("mqtt transport: unsupported broker scheme %q", parsed.Scheme)
	}
	host := parsed.Host
	if host == "" {
		return "", fmt.Errorf("mqtt transport: broker URL %q has no host", raw)
	}
	if parsed.Port() == "" {
		host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(1883))
	}
	return host, nil
}
