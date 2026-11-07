package mqtt

import (
	"encoding/json"
	"strconv"

	rpc "github.com/daritas/rpc-go"
)

const (
	FrameVersion  = "2"
	DefaultPrefix = "msgrpc/v2"
)

const (
	MRVersion         = "mr-v"
	MRSource          = "mr-src"
	MRKind            = "mr-kind"
	MRPath            = "mr-path"
	MRMethod          = "mr-method"
	MREvent           = "mr-event"
	MRCode            = "mr-code"
	MRNonce           = "mr-nonce"
	MRTimestamp       = "mr-ts"
	MRSignature       = "mr-sig"
	MRContractVersion = "mr-ver"
	MRTTL             = "mr-ttl"
	MRIdempotencyKey  = "mr-idem"
)

const (
	PresenceOnline  = "online"
	PresenceOffline = "offline"
)

func Topic(prefix string, channel Channel, peer string) string {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return prefix + "/" + string(channel) + "/" + peer
}

func PresenceTopic(prefix, peer string) string {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return prefix + "/presence/" + peer
}

func (f Frame) ControlProperties(source string) map[string]string {
	properties := map[string]string{
		MRVersion: FrameVersion,
		MRSource:  source,
		MRKind:    string(f.Kind),
	}
	if f.Path != "" {
		properties[MRPath] = f.Path
	}
	if f.Method != "" {
		properties[MRMethod] = f.Method
	}
	if f.Event != "" {
		properties[MREvent] = f.Event
	}
	if f.Code != "" {
		properties[MRCode] = string(f.Code)
	}
	if f.Version != "" {
		properties[MRContractVersion] = f.Version
	}
	if f.TTL > 0 {
		properties[MRTTL] = strconv.FormatInt(f.TTL, 10)
	}
	if f.IdempotencyKey != "" {
		properties[MRIdempotencyKey] = f.IdempotencyKey
	}
	return properties
}

func FrameFromControlProperties(properties map[string]string, body any, correlation string) (Frame, bool) {
	if properties[MRVersion] != FrameVersion {
		return Frame{}, false
	}
	kind := Kind(properties[MRKind])
	if kind == "" {
		return Frame{}, false
	}
	frame := Frame{
		Kind:           kind,
		Correlation:    correlation,
		Path:           properties[MRPath],
		Method:         properties[MRMethod],
		Event:          properties[MREvent],
		Code:           rpc.ErrorCode(properties[MRCode]),
		Version:        properties[MRContractVersion],
		IdempotencyKey: properties[MRIdempotencyKey],
		Body:           body,
	}
	if ttl := properties[MRTTL]; ttl != "" {
		parsed, err := strconv.ParseInt(ttl, 10, 64)
		if err != nil || parsed < 0 {
			return Frame{}, false
		}
		frame.TTL = parsed
	}
	switch kind {
	case KindCall, KindSubscribe, KindUnsubscribe:
		frame.Channel = ChannelRequest
	case KindResult, KindError:
		frame.Channel = ChannelResponse
	case KindEvent:
		frame.Channel = ChannelEvent
	default:
		return Frame{}, false
	}
	return frame, true
}

type Kind string

const (
	KindCall        Kind = "call"
	KindSubscribe   Kind = "subscribe"
	KindUnsubscribe Kind = "unsubscribe"
	KindResult      Kind = "result"
	KindError       Kind = "error"
	KindEvent       Kind = "event"
)

type Channel string

const (
	ChannelRequest  Channel = "req"
	ChannelResponse Channel = "rsp"
	ChannelEvent    Channel = "evt"
)

type Frame struct {
	Kind           Kind
	Channel        Channel
	Correlation    string
	Path           string
	Method         string
	Event          string
	Code           rpc.ErrorCode
	Version        string
	TTL            int64
	IdempotencyKey string
	Body           any
}

func RequestKind(method string) Kind {
	switch method {
	case "on":
		return KindSubscribe
	case "off", "removeListener":
		return KindUnsubscribe
	default:
		return KindCall
	}
}

func IsRequestKind(kind Kind) bool {
	return kind == KindCall || kind == KindSubscribe || kind == KindUnsubscribe
}

func methodOrEvent(frame Frame) string {
	if frame.Method != "" {
		return frame.Method
	}
	return frame.Event
}

func methodOrEventProperties(properties map[string]string) string {
	if properties[MRMethod] != "" {
		return properties[MRMethod]
	}
	return properties[MREvent]
}

func ToFrame(message rpc.Message) (Frame, bool) {
	switch payload := message.Payload.(type) {
	case rpc.CallPayload:
		return Frame{
			Kind:           RequestKind(payload.Method),
			Channel:        ChannelRequest,
			Correlation:    payload.ID,
			Path:           payload.Path,
			Method:         payload.Method,
			Version:        payload.Version,
			TTL:            payload.TTL,
			IdempotencyKey: payload.IdempotencyKey,
			Body:           payload.Params,
		}, true
	case rpc.SuccessPayload:
		return Frame{
			Kind:        KindResult,
			Channel:     ChannelResponse,
			Correlation: payload.ID,
			Body:        payload.Result,
		}, true
	case rpc.ErrorPayload:
		return Frame{
			Kind:        KindError,
			Channel:     ChannelResponse,
			Correlation: payload.ID,
			Code:        payload.Code,
			Body:        payload.Error,
		}, true
	case rpc.EventPayload:
		return Frame{
			Kind:    KindEvent,
			Channel: ChannelEvent,
			Path:    payload.Path,
			Event:   payload.Event,
			Body:    payload.Params,
		}, true
	default:
		return Frame{}, false
	}
}

func FromFrame(frame Frame) (rpc.Message, bool) {
	switch frame.Kind {
	case KindCall, KindSubscribe, KindUnsubscribe:
		if frame.Correlation == "" || frame.Path == "" || frame.Method == "" {
			return rpc.Message{}, false
		}
		params, ok := frame.Body.([]any)
		if !ok {
			if frame.Body == nil {
				params = []any{}
			} else {
				params = []any{frame.Body}
			}
		}
		return rpc.Message{
			Type: rpc.MessageTypeRequest,
			Payload: rpc.CallPayload{
				ID:             frame.Correlation,
				Type:           rpc.RPCMessageCall,
				Path:           frame.Path,
				Method:         frame.Method,
				Params:         params,
				Version:        frame.Version,
				TTL:            frame.TTL,
				IdempotencyKey: frame.IdempotencyKey,
			},
		}, true
	case KindResult:
		if frame.Correlation == "" {
			return rpc.Message{}, false
		}
		return rpc.Message{
			Type:    rpc.MessageTypeResponse,
			Payload: rpc.NewSuccessPayload(frame.Correlation, frame.Body),
		}, true
	case KindError:
		if frame.Correlation == "" {
			return rpc.Message{}, false
		}
		remote := remoteError(frame.Body)
		return rpc.Message{
			Type: rpc.MessageTypeError,
			Payload: rpc.ErrorPayload{
				ID:    frame.Correlation,
				Type:  rpc.RPCMessageError,
				Code:  frame.Code,
				Error: remote,
			},
		}, true
	case KindEvent:
		if frame.Event == "" {
			return rpc.Message{}, false
		}
		params, ok := frame.Body.([]any)
		if !ok {
			params = []any{frame.Body}
		}
		return rpc.Message{
			Type: rpc.MessageTypeEvent,
			Payload: rpc.EventPayload{
				Type:   rpc.RPCMessageEvent,
				Event:  frame.Event,
				Path:   frame.Path,
				Params: params,
			},
		}, true
	default:
		return rpc.Message{}, false
	}
}

func remoteError(body any) *rpc.RemoteError {
	switch value := body.(type) {
	case nil:
		return nil
	case *rpc.RemoteError:
		return value
	case rpc.RemoteError:
		return &value
	default:
		data, err := json.Marshal(value)
		if err != nil {
			return &rpc.RemoteError{Name: "Error", Message: "unreadable remote error"}
		}
		var remote rpc.RemoteError
		if err := json.Unmarshal(data, &remote); err != nil {
			return &rpc.RemoteError{Name: "Error", Message: "unreadable remote error"}
		}
		return &remote
	}
}
