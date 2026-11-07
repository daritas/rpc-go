package mqtt

import (
	"testing"

	rpc "github.com/daritas/rpc-go"
)

func TestCallToFrame(t *testing.T) {
	frame, ok := ToFrame(rpc.Message{
		Type: rpc.MessageTypeRequest,
		Payload: rpc.CallPayload{
			ID:             "request-1",
			Type:           rpc.RPCMessageCall,
			Path:           "plant",
			Method:         "writeSetpoint",
			Params:         []any{1200},
			Version:        "3",
			TTL:            10000,
			IdempotencyKey: "operator-command-1",
		},
	})
	if !ok {
		t.Fatal("ToFrame returned false")
	}
	if frame.Channel != ChannelRequest {
		t.Fatalf("channel = %s, want %s", frame.Channel, ChannelRequest)
	}
	if frame.Kind != KindCall {
		t.Fatalf("kind = %s, want %s", frame.Kind, KindCall)
	}
	if frame.Correlation != "request-1" || frame.Path != "plant" || frame.Method != "writeSetpoint" {
		t.Fatalf("frame addressing mismatch: %#v", frame)
	}
	if frame.Version != "3" || frame.TTL != 10000 || frame.IdempotencyKey != "operator-command-1" {
		t.Fatalf("frame call properties mismatch: %#v", frame)
	}
}

func TestSubscribeMethodMapsToSubscribeKind(t *testing.T) {
	frame, ok := ToFrame(rpc.Message{
		Type: rpc.MessageTypeRequest,
		Payload: rpc.CallPayload{
			ID:     "request-1",
			Type:   rpc.RPCMessageCall,
			Path:   "plant",
			Method: "on",
			Params: []any{"alarm"},
		},
	})
	if !ok {
		t.Fatal("ToFrame returned false")
	}
	if frame.Kind != KindSubscribe {
		t.Fatalf("kind = %s, want %s", frame.Kind, KindSubscribe)
	}
}

func TestFrameToErrorMessage(t *testing.T) {
	message, ok := FromFrame(Frame{
		Kind:        KindError,
		Channel:     ChannelResponse,
		Correlation: "request-1",
		Code:        rpc.ErrorForbidden,
		Body:        &rpc.RemoteError{Name: "RpcError", Message: "not permitted"},
	})
	if !ok {
		t.Fatal("FromFrame returned false")
	}
	failure, ok := message.Payload.(rpc.ErrorPayload)
	if !ok {
		t.Fatalf("payload = %T, want rpc.ErrorPayload", message.Payload)
	}
	if failure.Code != rpc.ErrorForbidden {
		t.Fatalf("code = %s, want %s", failure.Code, rpc.ErrorForbidden)
	}
}

func TestControlPropertiesRoundTrip(t *testing.T) {
	frame := Frame{
		Kind:           KindCall,
		Channel:        ChannelRequest,
		Correlation:    "request-1",
		Path:           "plant",
		Method:         "writeSetpoint",
		Version:        "3",
		TTL:            10000,
		IdempotencyKey: "operator-command-1",
		Body:           []any{1200},
	}

	properties := frame.ControlProperties("hmi")
	if properties[MRVersion] != FrameVersion || properties[MRSource] != "hmi" {
		t.Fatalf("properties missing version/source: %#v", properties)
	}

	roundTrip, ok := FrameFromControlProperties(properties, frame.Body, frame.Correlation)
	if !ok {
		t.Fatal("FrameFromControlProperties returned false")
	}
	if roundTrip.Kind != frame.Kind || roundTrip.Channel != frame.Channel || roundTrip.TTL != frame.TTL {
		t.Fatalf("round trip mismatch: %#v", roundTrip)
	}
}
