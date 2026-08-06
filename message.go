package rpc

import "fmt"

type MessageType string

const (
	MessageTypeRequest  MessageType = "REQUEST"
	MessageTypeResponse MessageType = "RESPONSE"
	MessageTypeError    MessageType = "ERROR"
	MessageTypeEvent    MessageType = "EVENT"
	MessageTypeUnknown  MessageType = "UNKNOWN"
)

type RPCMessageType string

const (
	RPCMessageCall    RPCMessageType = "POST"
	RPCMessageSuccess RPCMessageType = "SUCCESS"
	RPCMessageError   RPCMessageType = "ERROR"
	RPCMessageEvent   RPCMessageType = "EVENT"
)

type MethodSemantics string

const (
	SemanticsQuery                MethodSemantics = "query"
	SemanticsIdempotentCommand    MethodSemantics = "idempotent-command"
	SemanticsNonRepeatableCommand MethodSemantics = "non-repeatable-command"
)

type MethodEffect string

const (
	EffectObserve       MethodEffect = "observe"
	EffectOperate       MethodEffect = "operate"
	EffectProgram       MethodEffect = "program"
	EffectSecurityAdmin MethodEffect = "security-admin"
)

type ErrorCode string

const (
	ErrorClassNotFound       ErrorCode = "ClassNotFound"
	ErrorMethodNotFound      ErrorCode = "MethodNotFound"
	ErrorException           ErrorCode = "Exception"
	ErrorTimeout             ErrorCode = "Timeout"
	ErrorTransport           ErrorCode = "TransportError"
	ErrorUnauthorized        ErrorCode = "Unauthorized"
	ErrorForbidden           ErrorCode = "Forbidden"
	ErrorInvalidParams       ErrorCode = "InvalidParams"
	ErrorIncompatibleVersion ErrorCode = "IncompatibleVersion"
	ErrorUnknownOutcome      ErrorCode = "UnknownOutcome"
	ErrorBusy                ErrorCode = "Busy"
	ErrorSuperseded          ErrorCode = "Superseded"
	ErrorNotInControl        ErrorCode = "NotInControl"
	ErrorOwnershipChanged    ErrorCode = "OwnershipChanged"
)

type Message struct {
	Type    MessageType `json:"type,omitempty" msgpack:"type,omitempty"`
	Payload any         `json:"payload,omitempty" msgpack:"payload,omitempty"`
}

type CallPayload struct {
	ID             string         `json:"id" msgpack:"id"`
	Type           RPCMessageType `json:"type" msgpack:"type"`
	Path           string         `json:"path" msgpack:"path"`
	Method         string         `json:"method" msgpack:"method"`
	Params         []any          `json:"params" msgpack:"params"`
	Version        string         `json:"version,omitempty" msgpack:"version,omitempty"`
	Fence          *OwnerFence    `json:"fence,omitempty" msgpack:"fence,omitempty"`
	TTL            int64          `json:"ttl,omitempty" msgpack:"ttl,omitempty"`
	IdempotencyKey string         `json:"idempotencyKey,omitempty" msgpack:"idempotencyKey,omitempty"`
}

type OwnerFence struct {
	OwnerEpoch string `json:"ownerEpoch" msgpack:"ownerEpoch"`
}

type RemoteError struct {
	Name    string `json:"name" msgpack:"name"`
	Message string `json:"message" msgpack:"message"`
	Stack   string `json:"stack,omitempty" msgpack:"stack,omitempty"`
}

type ErrorPayload struct {
	ID    string         `json:"id" msgpack:"id"`
	Type  RPCMessageType `json:"type" msgpack:"type"`
	Code  ErrorCode      `json:"code" msgpack:"code"`
	Error *RemoteError   `json:"error,omitempty" msgpack:"error,omitempty"`
}

type SuccessPayload struct {
	ID     string         `json:"id" msgpack:"id"`
	Type   RPCMessageType `json:"type" msgpack:"type"`
	Result any            `json:"result" msgpack:"result"`
}

type EventPayload struct {
	Type   RPCMessageType `json:"type" msgpack:"type"`
	Event  string         `json:"event" msgpack:"event"`
	Params []any          `json:"params" msgpack:"params"`
	Path   string         `json:"path,omitempty" msgpack:"path,omitempty"`
	Seq    uint64         `json:"seq,omitempty" msgpack:"seq,omitempty"`
	Epoch  string         `json:"epoch,omitempty" msgpack:"epoch,omitempty"`
}

type RPCError struct {
	Code ErrorCode
	Err  *RemoteError
}

func (e *RPCError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil || e.Err.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Err.Message)
}

func NewErrorPayload(id string, code ErrorCode, message string) ErrorPayload {
	return ErrorPayload{
		ID:   id,
		Type: RPCMessageError,
		Code: code,
		Error: &RemoteError{
			Name:    "RpcError",
			Message: message,
		},
	}
}

func NewSuccessPayload(id string, result any) SuccessPayload {
	return SuccessPayload{
		ID:     id,
		Type:   RPCMessageSuccess,
		Result: result,
	}
}
