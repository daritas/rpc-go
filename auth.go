package rpc

type Identity struct {
	Name   string         `json:"name" msgpack:"name"`
	Roles  []string       `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Claims map[string]any `json:"claims,omitempty" msgpack:"claims,omitempty"`
}
