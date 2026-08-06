package rpc

import (
	"context"
	"reflect"
)

type NamespaceOption func(*NamespaceOptions)

func NamespaceVersion(version string) NamespaceOption {
	return func(options *NamespaceOptions) {
		options.Version = version
	}
}

func NamespaceClassName(className string) NamespaceOption {
	return func(options *NamespaceOptions) {
		options.ClassName = className
	}
}

func NamespaceEmitter(emitter bool) NamespaceOption {
	return func(options *NamespaceOptions) {
		options.Emitter = emitter
	}
}

type MethodOption func(*methodOptions)

type methodOptions struct {
	wireName          string
	params            []TypeNode
	paramNames        []string
	returns           TypeNode
	semantics         MethodSemantics
	requiresAuthority bool
}

func WireName(name string) MethodOption {
	return func(options *methodOptions) {
		options.wireName = name
	}
}

func Params(params ...TypeNode) MethodOption {
	return func(options *methodOptions) {
		options.params = params
	}
}

func ParamNames(names ...string) MethodOption {
	return func(options *methodOptions) {
		options.paramNames = names
	}
}

func Returns(returns TypeNode) MethodOption {
	return func(options *methodOptions) {
		options.returns = returns
	}
}

func Semantics(semantics MethodSemantics) MethodOption {
	return func(options *methodOptions) {
		options.semantics = semantics
	}
}

func RequiresAuthority(required bool) MethodOption {
	return func(options *methodOptions) {
		options.requiresAuthority = required
	}
}

type EventOption func(*EventDescription)

func Event(name string, params ...TypeNode) EventDescription {
	return EventDescription{Name: name, Params: params}
}

type MethodBinding struct {
	GoName   string
	Handler  Handler
	Function any
	Options  []MethodOption
}

func Method(name string, function any, options ...MethodOption) MethodBinding {
	return MethodBinding{GoName: name, Function: function, Options: options}
}

func ReceiverMethod(goName string, options ...MethodOption) MethodBinding {
	return MethodBinding{GoName: goName, Options: options}
}

type ServiceOptions struct {
	Namespace NamespaceOptions
	Methods   []MethodBinding
	Events    []EventDescription
}

type ServiceOption func(*ServiceOptions)

func WithNamespace(options ...NamespaceOption) ServiceOption {
	return func(service *ServiceOptions) {
		for _, option := range options {
			option(&service.Namespace)
		}
	}
}

func WithMethods(methods ...MethodBinding) ServiceOption {
	return func(service *ServiceOptions) {
		service.Methods = append(service.Methods, methods...)
	}
}

func WithEvents(events ...EventDescription) ServiceOption {
	return func(service *ServiceOptions) {
		service.Events = append(service.Events, events...)
	}
}

func describeMethod(goName string, options []MethodOption) MethodDescription {
	settings := methodOptions{wireName: goName}
	for _, option := range options {
		option(&settings)
	}
	return MethodDescription{
		Name:              settings.wireName,
		Params:            settings.params,
		ParamNames:        settings.paramNames,
		Returns:           settings.returns,
		Semantics:         settings.semantics,
		RequiresAuthority: settings.requiresAuthority,
	}
}

func handlerFromFunction(function any, name string) Handler {
	value := reflect.ValueOf(function)
	if !value.IsValid() || value.Kind() != reflect.Func {
		return func(context.Context, []any) (any, error) {
			return nil, &RPCError{Code: ErrorInvalidParams, Err: &RemoteError{Name: "RpcError", Message: name + " is not a function"}}
		}
	}
	return reflectedHandler(value, name)
}
