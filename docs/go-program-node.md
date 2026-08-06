# Go Program Node

`package node` exposes a small remote runtime for Go programs over the normal Source RPC server.
It is intended for MQTT-connected nodes where a trusted controller, and later MCP, can save,
build, start, stop and inspect Go programs on the node.

## Namespace

The namespace is `msgrpc.node`, class `GoProgramNode`, version `1`.

Read methods:

```text
describeRuntime() -> { directory, goCommand, namespace }
listPrograms() -> ProgramInfo[]
readProgram(name) -> string
programStatus(name) -> ProgramInfo
programOutput(name) -> OutputResult
```

Mutation methods require an authenticated identity:

```text
saveProgram(name, source) -> ProgramFile
deleteProgram(name) -> string
buildProgram(name) -> BuildResult
startProgram(name) -> StartResult
stopProgram(name) -> StopResult
```

`saveProgram` also accepts an optional third object `{ goMod }` for callers that need to provide a
specific module file. If omitted, the node writes a minimal module next to `main.go`.

## Runtime Behavior

Programs are stored as one directory per safe program name. Names may contain letters, digits,
periods, underscores and hyphens, and may not contain path separators.

Each saved program has:

```text
<programs>/<name>/main.go
<programs>/<name>/go.mod
<programs>/.bin/<name>
<programs>/.gocache/
```

`startProgram` builds the binary if it is missing or older than `main.go`, then starts the binary as
an independent process. The RPC call deadline applies to build/start setup, not to the lifetime of
the launched program. Use `stopProgram` or node shutdown to stop long-running programs.

The runner captures stdout and stderr into a bounded in-memory ring. Stderr lines are prefixed with
`! ` in `programOutput`.

## Exposing A Node

```go
server := rpc.NewServer("goProgramNode")
options := node.ServiceOptions{
    Runner: node.RunnerOptions{Directory: "programs"},
    Allow:  []string{"controller"},
}
service := node.NewService(options)
defer service.Close()

if err := service.Expose(server); err != nil {
    log.Fatal(err)
}
server.UseAuthorizer(node.Authorizer(options, nil))
```

For MQTT, configure signing on the transport so mutating methods receive an identity. The example
server supports a simple shared-secret setup:

```sh
SOURCE_RPC_SHARED_SECRET=dev-secret \
SOURCE_RPC_SCRIPTABLE_BY=controller \
go run ./examples/program_node
```

Without a verifier, MQTT callers can still read public runtime state, but `saveProgram`,
`buildProgram`, `startProgram`, `stopProgram` and `deleteProgram` return `Unauthorized`.
