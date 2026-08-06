package rpc

import "context"

type Identity struct {
	Name   string         `json:"name" msgpack:"name"`
	Roles  []string       `json:"roles,omitempty" msgpack:"roles,omitempty"`
	Claims map[string]any `json:"claims,omitempty" msgpack:"claims,omitempty"`
}

type CallContext struct {
	Source       string
	Identity     *Identity
	InstanceName string
	Method       string
	Params       []any
	Subscription bool
}

type Authorizer func(context.Context, CallContext) (bool, error)

type callContextKey struct{}

func ContextWithCallContext(ctx context.Context, callContext CallContext) context.Context {
	return context.WithValue(ctx, callContextKey{}, callContext)
}

func CallContextFromContext(ctx context.Context) (CallContext, bool) {
	callContext, ok := ctx.Value(callContextKey{}).(CallContext)
	return callContext, ok
}
