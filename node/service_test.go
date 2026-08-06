package node

import (
	"context"
	"strings"
	"testing"
	"time"

	rpc "github.com/source-repo/rpc-go"
)

func TestRunnerSavesBuildsStartsAndCapturesOutput(t *testing.T) {
	runner := newTestRunner(t)
	source := `package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Println("hello from go")
	fmt.Fprintln(os.Stderr, "careful")
}
`
	file, err := runner.Save("hello", source, nil)
	if err != nil {
		t.Fatal(err)
	}
	if file.Name != "hello" || file.Path == "" {
		t.Fatalf("file = %#v, want hello file", file)
	}
	read, err := runner.Read("hello")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(read, "hello from go") {
		t.Fatalf("read = %q, want saved source", read)
	}
	built, err := runner.Build(context.Background(), "hello")
	if err != nil {
		t.Fatalf("build output: %v\n%v", err, built.Output)
	}
	if built.Binary == "" {
		t.Fatal("built binary path is empty")
	}
	started, err := runner.Start(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if started.PID == 0 {
		t.Fatalf("started = %#v, want pid", started)
	}
	output := waitForOutput(t, runner, "hello", func(output OutputResult) bool {
		return !output.Running && strings.Contains(strings.Join(output.Output, "\n"), "hello from go")
	})
	joined := strings.Join(output.Output, "\n")
	if !strings.Contains(joined, "! careful") {
		t.Fatalf("output = %#v, want stderr line with prefix", output.Output)
	}
	list, err := runner.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Name != "hello" || list[0].Ended == nil {
		t.Fatalf("list = %#v, want finished hello", list)
	}
}

func TestRunnerStopsRunningProgram(t *testing.T) {
	runner := newTestRunner(t)
	source := `package main

import (
	"fmt"
	"time"
)

func main() {
	for {
		fmt.Println("tick")
		time.Sleep(50 * time.Millisecond)
	}
}
`
	if _, err := runner.Save("ticker", source, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Start(context.Background(), "ticker"); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, runner, "ticker", func(output OutputResult) bool {
		return len(output.Output) > 0
	})
	stopped, err := runner.Stop("ticker")
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Ended == nil {
		t.Fatalf("stopped = %#v, want exit state", stopped)
	}
	output, err := runner.Output("ticker")
	if err != nil {
		t.Fatal(err)
	}
	if output.Running {
		t.Fatalf("output = %#v, want stopped program", output)
	}
}

func TestRunnerStartRebuildsStaleBinary(t *testing.T) {
	runner := newTestRunner(t)
	first := `package main

import "fmt"

func main() {
	fmt.Println("first")
}
`
	second := `package main

import "fmt"

func main() {
	fmt.Println("second")
}
`
	if _, err := runner.Save("reload", first, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Start(context.Background(), "reload"); err != nil {
		t.Fatal(err)
	}
	waitForOutput(t, runner, "reload", func(output OutputResult) bool {
		return !output.Running && strings.Contains(strings.Join(output.Output, "\n"), "first")
	})

	time.Sleep(10 * time.Millisecond)
	if _, err := runner.Save("reload", second, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Start(context.Background(), "reload"); err != nil {
		t.Fatal(err)
	}
	output := waitForOutput(t, runner, "reload", func(output OutputResult) bool {
		return !output.Running && strings.Contains(strings.Join(output.Output, "\n"), "second")
	})
	if strings.Contains(strings.Join(output.Output, "\n"), "first") {
		t.Fatalf("output = %#v, want rebuilt program only", output.Output)
	}
}

func TestRunnerRejectsUnsafeNames(t *testing.T) {
	runner := newTestRunner(t)
	if _, err := runner.Save("../nope", "package main\nfunc main(){}\n", nil); err == nil {
		t.Fatal("Save accepted an unsafe name")
	}
}

func TestServiceReturnsCompilerDiagnostics(t *testing.T) {
	service := NewService(ServiceOptions{Runner: RunnerOptions{Directory: t.TempDir()}})
	server := rpc.NewServer("node")
	if err := service.Expose(server); err != nil {
		t.Fatal(err)
	}
	identity := &rpc.Identity{Name: "controller"}
	server.DispatchWithIdentity(context.Background(), "controller", identity, rpc.CallPayload{
		ID:     "save-invalid",
		Type:   rpc.RPCMessageCall,
		Path:   Namespace,
		Method: "saveProgram",
		Params: []any{"invalid", "package main\nfunc main() { missing() }\n"},
	})

	reply := server.DispatchWithIdentity(context.Background(), "controller", identity, rpc.CallPayload{
		ID:     "build-invalid",
		Type:   rpc.RPCMessageCall,
		Path:   Namespace,
		Method: "buildProgram",
		Params: []any{"invalid"},
	})
	failure, ok := reply.(rpc.ErrorPayload)
	if !ok {
		t.Fatalf("reply = %T, want ErrorPayload", reply)
	}
	if failure.Error == nil || !strings.Contains(failure.Error.Message, "undefined: missing") {
		t.Fatalf("error = %#v, want compiler diagnostic", failure.Error)
	}
}

func TestServiceExposesNodeNamespaceWithAuthority(t *testing.T) {
	service := NewService(ServiceOptions{Runner: RunnerOptions{Directory: t.TempDir()}})
	server := rpc.NewServer("node")
	if err := service.Expose(server); err != nil {
		t.Fatal(err)
	}

	description := server.Describe()
	var node rpc.NamespaceDescription
	for _, namespace := range description.Namespaces {
		if namespace.Name == Namespace {
			node = namespace
		}
	}
	if node.Name == "" {
		t.Fatal("node namespace missing")
	}
	protected := map[string]bool{}
	for _, method := range node.Methods {
		protected[method.Name] = method.RequiresAuthority
	}
	if !protected["saveProgram"] || protected["listPrograms"] {
		t.Fatalf("protected = %#v, want saveProgram protected and listPrograms open", protected)
	}

	reply := server.Dispatch(context.Background(), "bench", rpc.CallPayload{
		ID:     "request-1",
		Type:   rpc.RPCMessageCall,
		Path:   Namespace,
		Method: "saveProgram",
		Params: []any{"hello", "package main\nfunc main(){}\n"},
	})
	failure, ok := reply.(rpc.ErrorPayload)
	if !ok {
		t.Fatalf("reply = %T, want ErrorPayload", reply)
	}
	if failure.Code != rpc.ErrorUnauthorized {
		t.Fatalf("code = %s, want Unauthorized", failure.Code)
	}

	reply = server.DispatchWithIdentity(context.Background(), "bench", &rpc.Identity{Name: "bench"}, rpc.CallPayload{
		ID:     "request-2",
		Type:   rpc.RPCMessageCall,
		Path:   Namespace,
		Method: "saveProgram",
		Params: []any{"hello", "package main\nfunc main(){}\n"},
	})
	if _, ok := reply.(rpc.SuccessPayload); !ok {
		t.Fatalf("reply = %T, want SuccessPayload", reply)
	}
}

func TestAuthorizerAllowsNamedAuthenticatedPeersOnly(t *testing.T) {
	guard := Authorizer(ServiceOptions{Allow: []string{"bench"}}, nil)
	allowed, err := guard(context.Background(), rpc.CallContext{
		Source:       "guest",
		InstanceName: Namespace,
		Method:       "listPrograms",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("read-only node method was denied")
	}
	allowed, err = guard(context.Background(), rpc.CallContext{
		Source:       "bench",
		InstanceName: Namespace,
		Method:       "saveProgram",
	})
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("unauthenticated caller was allowed")
	}
	allowed, err = guard(context.Background(), rpc.CallContext{
		Source:       "stranger",
		Identity:     &rpc.Identity{Name: "stranger"},
		InstanceName: Namespace,
		Method:       "saveProgram",
	})
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("unnamed peer was allowed")
	}
	allowed, err = guard(context.Background(), rpc.CallContext{
		Source:       "bench",
		Identity:     &rpc.Identity{Name: "bench"},
		InstanceName: Namespace,
		Method:       "saveProgram",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("named authenticated peer was refused")
	}

	guard = Authorizer(ServiceOptions{}, nil)
	allowed, err = guard(context.Background(), rpc.CallContext{
		Source:       "bench",
		Identity:     &rpc.Identity{Name: "bench"},
		InstanceName: Namespace,
		Method:       "saveProgram",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("authenticated peer was refused without an allow list")
	}
}

func newTestRunner(t *testing.T) *Runner {
	t.Helper()
	return NewRunner(RunnerOptions{Directory: t.TempDir(), BuildTimeout: 10 * time.Second, StopTimeout: time.Second, MaxOutputLines: 20})
}

func waitForOutput(t *testing.T, runner *Runner, name string, done func(OutputResult) bool) OutputResult {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last OutputResult
	for time.Now().Before(deadline) {
		output, err := runner.Output(name)
		if err == nil {
			last = output
			if done(output) {
				return output
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for output, last = %#v", last)
	return OutputResult{}
}
