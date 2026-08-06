package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

type Handler func(context.Context, []any) (any, error)

type Server struct {
	name       string
	methods    map[string]map[string]Handler
	semantics  map[string]map[string]MethodSemantics
	namespaces map[string]NamespaceOptions
	described  map[string]map[string]MethodDescription
	events     map[string]map[string]EventDescription
	receivedAt func() time.Time
}

func NewServer(name string) *Server {
	server := &Server{
		name:       name,
		methods:    map[string]map[string]Handler{},
		semantics:  map[string]map[string]MethodSemantics{},
		namespaces: map[string]NamespaceOptions{},
		described:  map[string]map[string]MethodDescription{},
		events:     map[string]map[string]EventDescription{},
		receivedAt: time.Now,
	}
	server.ExposeFunc("msgrpc", "describe", func(context.Context, []any) (any, error) {
		return server.Describe(), nil
	})
	server.DescribeMethod("msgrpc", MethodDescription{
		Name:      "describe",
		Params:    []TypeNode{},
		Returns:   TypeNode{"kind": "ref", "name": "msgrpc.ServerDescription"},
		Semantics: SemanticsQuery,
	})
	server.SetNamespace("msgrpc", NamespaceOptions{ClassName: "Introspection"})
	return server
}

func (s *Server) Name() string {
	return s.name
}

func (s *Server) ExposeFunc(namespace, method string, handler Handler) {
	if s.methods[namespace] == nil {
		s.methods[namespace] = map[string]Handler{}
	}
	s.methods[namespace][method] = handler
	if s.described[namespace] == nil {
		s.described[namespace] = map[string]MethodDescription{}
	}
	if _, exists := s.described[namespace][method]; !exists {
		s.described[namespace][method] = MethodDescription{Name: method}
	}
}

func (s *Server) SetNamespace(namespace string, options NamespaceOptions) {
	s.namespaces[namespace] = options
}

func (s *Server) DescribeMethod(namespace string, description MethodDescription) {
	if description.Name == "" {
		return
	}
	if s.described[namespace] == nil {
		s.described[namespace] = map[string]MethodDescription{}
	}
	if description.Semantics != "" {
		s.DeclareSemantics(namespace, description.Name, description.Semantics)
	}
	s.described[namespace][description.Name] = description
}

func (s *Server) DescribeEvent(namespace string, description EventDescription) {
	if description.Name == "" {
		return
	}
	if s.events[namespace] == nil {
		s.events[namespace] = map[string]EventDescription{}
	}
	s.events[namespace][description.Name] = description
}

func (s *Server) HasEvent(namespace, event string) bool {
	if len(s.events[namespace]) == 0 {
		return false
	}
	_, ok := s.events[namespace][event]
	return ok
}

func (s *Server) DeclareSemantics(namespace, method string, semantics MethodSemantics) {
	if s.semantics[namespace] == nil {
		s.semantics[namespace] = map[string]MethodSemantics{}
	}
	s.semantics[namespace][method] = semantics
}

func (s *Server) Semantics(namespace, method string) (MethodSemantics, bool) {
	semantics, ok := s.semantics[namespace][method]
	return semantics, ok
}

func (s *Server) Describe() ServerDescription {
	namespaces := make([]string, 0, len(s.methods))
	for namespace := range s.methods {
		namespaces = append(namespaces, namespace)
	}
	sort.Strings(namespaces)

	description := ServerDescription{
		Name:       s.name,
		Validating: false,
		Namespaces: make([]NamespaceDescription, 0, len(namespaces)),
	}
	for _, namespace := range namespaces {
		options := s.namespaces[namespace]
		methods := make([]MethodDescription, 0, len(s.methods[namespace]))
		names := make([]string, 0, len(s.methods[namespace]))
		for name := range s.methods[namespace] {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			method := s.described[namespace][name]
			if method.Name == "" {
				method.Name = name
			}
			if method.Semantics == "" {
				if semantics, ok := s.Semantics(namespace, name); ok {
					method.Semantics = semantics
				}
			}
			methods = append(methods, method)
		}
		events := make([]EventDescription, 0, len(s.events[namespace]))
		eventNames := make([]string, 0, len(s.events[namespace]))
		for name := range s.events[namespace] {
			eventNames = append(eventNames, name)
		}
		sort.Strings(eventNames)
		for _, name := range eventNames {
			events = append(events, s.events[namespace][name])
		}
		description.Namespaces = append(description.Namespaces, NamespaceDescription{
			Name:      namespace,
			Version:   options.Version,
			ClassName: options.ClassName,
			Created:   false,
			Emitter:   options.Emitter,
			Methods:   methods,
			Events:    events,
		})
	}
	return description
}

func (s *Server) Expose(namespace string, receiver any) error {
	value := reflect.ValueOf(receiver)
	if !value.IsValid() {
		return fmt.Errorf("expose %s: nil receiver", namespace)
	}
	typ := value.Type()
	for i := 0; i < typ.NumMethod(); i++ {
		method := typ.Method(i)
		if method.PkgPath != "" {
			continue
		}
		s.ExposeFunc(namespace, method.Name, reflectedHandler(value.Method(i), method.Name))
	}
	if len(s.methods[namespace]) == 0 {
		return fmt.Errorf("expose %s: no exported methods on %T", namespace, receiver)
	}
	return nil
}

func (s *Server) Dispatch(ctx context.Context, source string, call CallPayload) any {
	arrived := s.receivedAt()
	return s.DispatchArrived(ctx, source, call, arrived)
}

func (s *Server) DispatchArrived(ctx context.Context, source string, call CallPayload, arrived time.Time) any {
	_ = source
	if call.Type != "" && call.Type != RPCMessageCall {
		return NewErrorPayload(call.ID, ErrorInvalidParams, "payload is not a call")
	}
	if call.TTL > 0 && time.Since(arrived) > time.Duration(call.TTL)*time.Millisecond {
		return NewErrorPayload(call.ID, ErrorTimeout, fmt.Sprintf("%s.%s was not run: its caller stopped waiting", call.Path, call.Method))
	}
	namespace := s.methods[call.Path]
	if namespace == nil {
		return NewErrorPayload(call.ID, ErrorClassNotFound, fmt.Sprintf("%s.%s is not exposed", call.Path, call.Method))
	}
	handler := namespace[call.Method]
	if handler == nil {
		return NewErrorPayload(call.ID, ErrorMethodNotFound, fmt.Sprintf("%s.%s is not exposed", call.Path, call.Method))
	}
	result, err := handler(ctx, call.Params)
	if err != nil {
		var rpcErr *RPCError
		if ok := AsRPCError(err, &rpcErr); ok && rpcErr != nil {
			return ErrorPayload{ID: call.ID, Type: RPCMessageError, Code: rpcErr.Code, Error: rpcErr.Err}
		}
		return NewErrorPayload(call.ID, ErrorException, err.Error())
	}
	return NewSuccessPayload(call.ID, result)
}

func reflectedHandler(method reflect.Value, name string) Handler {
	methodType := method.Type()
	return func(ctx context.Context, params []any) (any, error) {
		args := make([]reflect.Value, 0, methodType.NumIn())
		paramOffset := 0
		if methodType.NumIn() > 0 && methodType.In(0) == reflect.TypeOf((*context.Context)(nil)).Elem() {
			args = append(args, reflect.ValueOf(ctx))
			paramOffset = 1
		}
		expected := methodType.NumIn() - paramOffset
		if len(params) != expected {
			return nil, fmt.Errorf("%s: expected %d argument(s), got %d", name, expected, len(params))
		}
		for i := 0; i < expected; i++ {
			value, err := coerce(params[i], methodType.In(i+paramOffset))
			if err != nil {
				return nil, fmt.Errorf("%s argument %d: %w", name, i, err)
			}
			args = append(args, value)
		}
		results := method.Call(args)
		return unpackResults(results)
	}
}

func coerce(value any, target reflect.Type) (reflect.Value, error) {
	if value == nil {
		switch target.Kind() {
		case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice:
			return reflect.Zero(target), nil
		default:
			return reflect.Value{}, fmt.Errorf("nil is not assignable to %s", target)
		}
	}
	got := reflect.ValueOf(value)
	if got.Type().AssignableTo(target) {
		return got, nil
	}
	if got.Type().ConvertibleTo(target) {
		return got.Convert(target), nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return reflect.Value{}, err
	}
	out := reflect.New(target)
	if err := json.Unmarshal(data, out.Interface()); err != nil {
		return reflect.Value{}, err
	}
	return out.Elem(), nil
}

func unpackResults(results []reflect.Value) (any, error) {
	if len(results) == 0 {
		return nil, nil
	}
	last := results[len(results)-1]
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	hasError := last.IsValid() && last.Type().Implements(errorType)
	if hasError && !last.IsNil() {
		return nil, last.Interface().(error)
	}
	if len(results) == 1 {
		if hasError {
			return nil, nil
		}
		return results[0].Interface(), nil
	}
	values := make([]any, 0, len(results))
	limit := len(results)
	if hasError {
		limit--
	}
	if limit == 1 {
		return results[0].Interface(), nil
	}
	for i := 0; i < limit; i++ {
		values = append(values, results[i].Interface())
	}
	return values, nil
}

func AsRPCError(err error, target **RPCError) bool {
	return errors.As(err, target)
}
