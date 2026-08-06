package rpc

import "encoding/json"

import "github.com/vmihailenco/msgpack/v5"

type Codec interface {
	ContentType() string
	Encode(value any) ([]byte, error)
	Decode(data []byte, value any) error
	DecodeValue(data []byte) (any, error)
}

type JSONCodec struct{}

func (JSONCodec) ContentType() string {
	return "application/json"
}

func (JSONCodec) Encode(value any) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONCodec) Decode(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func (JSONCodec) DecodeValue(data []byte) (any, error) {
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}

type MsgPackCodec struct{}

func (MsgPackCodec) ContentType() string {
	return "application/msgpack"
}

func (MsgPackCodec) Encode(value any) ([]byte, error) {
	return msgpack.Marshal(value)
}

func (MsgPackCodec) Decode(data []byte, value any) error {
	return msgpack.Unmarshal(data, value)
}

func (MsgPackCodec) DecodeValue(data []byte) (any, error) {
	var value any
	if err := msgpack.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	return value, nil
}
