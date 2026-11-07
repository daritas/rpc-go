package node

import (
	"context"
	"encoding/json"
	"fmt"

	rpc "github.com/daritas/rpc-go"
)

type ServiceOptions struct {
	Runner RunnerOptions
	Allow  []string
}

type Service struct {
	runner *Runner
}

var mutationMethods = map[string]struct{}{
	"saveProgram":   {},
	"deleteProgram": {},
	"buildProgram":  {},
	"startProgram":  {},
	"stopProgram":   {},
}

func NewService(options ServiceOptions) *Service {
	return &Service{runner: NewRunner(options.Runner)}
}

func (s *Service) Expose(server *rpc.Server) error {
	return server.ExposeService(Namespace, s,
		rpc.WithNamespace(rpc.NamespaceVersion("1"), rpc.NamespaceClassName("GoProgramNode")),
		rpc.WithMethods(
			rpc.ReceiverMethod("DescribeRuntime", rpc.WireName("describeRuntime"), rpc.Semantics(rpc.SemanticsQuery), rpc.Returns(rpc.TypeAny())),
			rpc.ReceiverMethod("List", rpc.WireName("listPrograms"), rpc.Semantics(rpc.SemanticsQuery), rpc.Returns(rpc.TypeAny())),
			rpc.MethodBinding{
				GoName:  "saveProgram",
				Handler: s.saveProgram,
				Options: []rpc.MethodOption{
					rpc.Semantics(rpc.SemanticsIdempotentCommand),
					rpc.RequiresAuthority(true),
					rpc.Params(rpc.TypeString(), rpc.TypeString()),
					rpc.Returns(rpc.TypeAny()),
				},
			},
			rpc.ReceiverMethod("Read", rpc.WireName("readProgram"), rpc.Semantics(rpc.SemanticsQuery), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeString())),
			rpc.ReceiverMethod("Remove", rpc.WireName("deleteProgram"), rpc.Semantics(rpc.SemanticsIdempotentCommand), rpc.RequiresAuthority(true), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeString())),
			rpc.ReceiverMethod("Build", rpc.WireName("buildProgram"), rpc.Semantics(rpc.SemanticsIdempotentCommand), rpc.RequiresAuthority(true), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeAny())),
			rpc.ReceiverMethod("Start", rpc.WireName("startProgram"), rpc.Semantics(rpc.SemanticsIdempotentCommand), rpc.RequiresAuthority(true), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeAny())),
			rpc.ReceiverMethod("Stop", rpc.WireName("stopProgram"), rpc.Semantics(rpc.SemanticsIdempotentCommand), rpc.RequiresAuthority(true), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeAny())),
			rpc.ReceiverMethod("Status", rpc.WireName("programStatus"), rpc.Semantics(rpc.SemanticsQuery), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeAny())),
			rpc.ReceiverMethod("Output", rpc.WireName("programOutput"), rpc.Semantics(rpc.SemanticsQuery), rpc.Params(rpc.TypeString()), rpc.Returns(rpc.TypeAny())),
		),
	)
}

func (s *Service) Close() {
	s.runner.StopAll()
}

func (s *Service) DescribeRuntime(context.Context) (RuntimeDescription, error) {
	return s.runner.Runtime(), nil
}

func (s *Service) List(context.Context) ([]ProgramInfo, error) {
	return s.runner.List()
}

func (s *Service) Save(_ context.Context, name string, source string) (ProgramFile, error) {
	return s.runner.Save(name, source, nil)
}

func (s *Service) saveProgram(_ context.Context, params []any) (any, error) {
	if len(params) != 2 && len(params) != 3 {
		return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "saveProgram expects name, source and optional options"}}
	}
	name, ok := params[0].(string)
	if !ok {
		return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "saveProgram name must be a string"}}
	}
	source, ok := params[1].(string)
	if !ok {
		return nil, &rpc.RPCError{Code: rpc.ErrorInvalidParams, Err: &rpc.RemoteError{Name: "RpcError", Message: "saveProgram source must be a string"}}
	}
	var options *SaveOptions
	if len(params) == 3 && params[2] != nil {
		var parsed SaveOptions
		data, err := json.Marshal(params[2])
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			return nil, fmt.Errorf("saveProgram options: %w", err)
		}
		options = &parsed
	}
	return s.runner.Save(name, source, options)
}

func (s *Service) Read(_ context.Context, name string) (string, error) {
	return s.runner.Read(name)
}

func (s *Service) Remove(_ context.Context, name string) (string, error) {
	return s.runner.Delete(name)
}

func (s *Service) Build(ctx context.Context, name string) (BuildResult, error) {
	return s.runner.Build(ctx, name)
}

func (s *Service) Start(ctx context.Context, name string) (StartResult, error) {
	return s.runner.Start(ctx, name)
}

func (s *Service) Stop(_ context.Context, name string) (StopResult, error) {
	return s.runner.Stop(name)
}

func (s *Service) Status(_ context.Context, name string) (ProgramInfo, error) {
	return s.runner.Status(name)
}

func (s *Service) Output(_ context.Context, name string) (OutputResult, error) {
	return s.runner.Output(name)
}

func Authorizer(options ServiceOptions, inner rpc.Authorizer) rpc.Authorizer {
	allowed := map[string]struct{}{}
	for _, name := range options.Allow {
		allowed[name] = struct{}{}
	}
	return func(ctx context.Context, call rpc.CallContext) (bool, error) {
		if call.InstanceName != Namespace {
			if inner == nil {
				return true, nil
			}
			return inner(ctx, call)
		}
		if _, mutating := mutationMethods[call.Method]; !mutating {
			if inner == nil {
				return true, nil
			}
			return inner(ctx, call)
		}
		if inner != nil {
			innerAllowed, err := inner(ctx, call)
			if err != nil || !innerAllowed {
				return innerAllowed, err
			}
		}
		if call.Identity == nil {
			return false, nil
		}
		if len(allowed) == 0 {
			return true, nil
		}
		_, ok := allowed[call.Identity.Name]
		return ok, nil
	}
}
