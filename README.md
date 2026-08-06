# Source RPC for Go

This is the start of a Go implementation of the `@source-repo/rpc` runtime.

The Go version is MQTT-first. The TypeScript package supports socket.io and MQTT 5, but the reason
to have a Go implementation is efficient MQTT peers: services, gateways, simulators and plant-side
nodes that sit on a broker. WebSocket/socket.io can stay out of the initial surface.

## Initial Shape

- `package rpc` contains the transport-neutral message vocabulary, codecs and server dispatch.
- `package mqtt` maps RPC messages to the documented MQTT 5 `msgrpc/v2` frame layout.
- The core starts with explicit calls and reflected service exposure. Typed clients can be written
  by hand or generated later.
- MsgPack is the default codec, matching the TypeScript package. JSON is also supported and replies
  mirror the request content type.
- Event subscriptions use the Source RPC convention: a client sends `on(event)`, the server records
  that peer as a subscriber, and events are published on `evt/<peer>`.

The first useful compatibility target is the MQTT 5 frame spec in `rpc-ts/docs/mqtt5-frame-spec.md`:
requests on `req`, responses on `rsp`, events on `evt`, and control fields in `mr-*` user
properties.

## Example Direction

```go
server := rpc.NewServer("plantServer")
server.Expose("plant", plant)

reply := server.Dispatch(ctx, "hmi", rpc.CallPayload{
    ID:     "request-1",
    Type:   rpc.RPCMessageCall,
    Path:   "plant",
    Method: "ReadSetpoint",
})
```

The MQTT transport will be responsible for carrying that call on:

```text
msgrpc/v2/req/plantServer
  mr-v=2
  mr-src=hmi
  mr-kind=call
  mr-path=plant
  mr-method=ReadSetpoint
```

## Testing With A Broker

The MQTT integration test uses `mqtt://localhost:1883` when it is reachable. If the broker requires
credentials, set:

```sh
MSGRPC_TEST_BROKER_USERNAME=...
MSGRPC_TEST_BROKER_PASSWORD=...
```

Set `SOURCE_RPC_REQUIRE_BROKER=1` to make broker absence or authentication failure a test failure
instead of a skip.

Set `MSGRPC_TEST_INTEROP_PEER=<peer>` to have the Go client call a live Source RPC peer exposing
`calculator.add(number, number)`.

`examples/calculator_server` serves `calculator.add` and emits `calculator.changed` with the sum.
