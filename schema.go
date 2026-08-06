package rpc

type TypeNode map[string]any

func TypeAny() TypeNode {
	return TypeNode{"kind": "any"}
}

func TypeBoolean() TypeNode {
	return TypeNode{"kind": "boolean"}
}

func TypeNumber() TypeNode {
	return TypeNode{"kind": "number"}
}

func TypeString() TypeNode {
	return TypeNode{"kind": "string"}
}

func TypeNull() TypeNode {
	return TypeNode{"kind": "null"}
}

type MethodDescription struct {
	Name              string          `json:"name" msgpack:"name"`
	Params            []TypeNode      `json:"params,omitempty" msgpack:"params,omitempty"`
	ParamNames        []string        `json:"paramNames,omitempty" msgpack:"paramNames,omitempty"`
	Returns           TypeNode        `json:"returns,omitempty" msgpack:"returns,omitempty"`
	Semantics         MethodSemantics `json:"semantics,omitempty" msgpack:"semantics,omitempty"`
	RequiresAuthority bool            `json:"requiresAuthority,omitempty" msgpack:"requiresAuthority,omitempty"`
}

type EventDescription struct {
	Name              string     `json:"name" msgpack:"name"`
	Params            []TypeNode `json:"params,omitempty" msgpack:"params,omitempty"`
	Subscribers       int        `json:"subscribers" msgpack:"subscribers"`
	RequiresAuthority bool       `json:"requiresAuthority,omitempty" msgpack:"requiresAuthority,omitempty"`
}

type NamespaceDescription struct {
	Name      string              `json:"name" msgpack:"name"`
	Version   string              `json:"version,omitempty" msgpack:"version,omitempty"`
	ClassName string              `json:"className,omitempty" msgpack:"className,omitempty"`
	Created   bool                `json:"created" msgpack:"created"`
	Emitter   bool                `json:"emitter" msgpack:"emitter"`
	Methods   []MethodDescription `json:"methods" msgpack:"methods"`
	Events    []EventDescription  `json:"events" msgpack:"events"`
}

type ServerDescription struct {
	Name       string                 `json:"name" msgpack:"name"`
	Validating bool                   `json:"validating" msgpack:"validating"`
	Namespaces []NamespaceDescription `json:"namespaces" msgpack:"namespaces"`
	Types      map[string]TypeNode    `json:"types,omitempty" msgpack:"types,omitempty"`
}

type NamespaceOptions struct {
	Version   string
	ClassName string
	Emitter   bool
}
