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
server.ExposeService("calculator", nil,
    rpc.WithNamespace(rpc.NamespaceVersion("1"), rpc.NamespaceClassName("Calculator")),
    rpc.WithMethods(rpc.Method("add", func(a, b float64) (float64, error) {
        return a + b, nil
    }, rpc.Params(rpc.TypeNumber(), rpc.TypeNumber()), rpc.Returns(rpc.TypeNumber()))),
})
```

The MQTT transport carries a call to that method on:

```text
msgrpc/v2/req/plantServer
  mr-v=2
  mr-src=hmi
  mr-kind=call
  mr-path=plant
  mr-method=ReadSetpoint
```

## Securing A Link

Three mechanisms, answering three different questions. They compose; none replaces another.

| mechanism | answers | where it belongs |
| --- | --- | --- |
| broker ACLs | may this peer publish or subscribe here? | inside one trust domain, always |
| TLS | can anyone on the path read or rewrite this? | any link leaving the host or the plant network |
| frame signing | who wrote this frame, and is it unaltered? | any link crossing a trust boundary |

Signing and TLS are the pair most easily confused. A signature proves authorship and integrity and
hides nothing: without TLS, every payload, every method name, and the username and password in the
CONNECT packet are readable by anyone on the path. Conversely TLS says the link reached the broker
you expected, and says nothing about which peer published a frame the broker then handed you. Where
signing is warranted, so is TLS.

Reach a broker over TLS with an `mqtts://` URL (port 8883 by default; `tls://` and `ssl://` also
work). With nothing else configured, the broker is verified against the host's trust store under the
name in the URL:

```go
transport := mqtt.NewTransport(mqtt.TransportOptions{
    Name: "plantServer",
    URL:  "mqtts://broker.example.com",
})
```

For a plant running its own certificate authority, or a broker that identifies peers by
certificate:

```go
transport := mqtt.NewTransport(mqtt.TransportOptions{
    Name: "plantServer",
    URL:  "mqtts://broker.plant.local",
    TLS: &mqtt.TLSOptions{
        CAFile:          "/etc/source-rpc/plant-ca.pem",
        CertificateFile: "/etc/source-rpc/plantServer.pem",
        KeyFile:         "/etc/source-rpc/plantServer-key.pem",
    },
})
```

`TLSOptions.Config` takes a `*tls.Config` outright for anything the convenience fields do not reach.
`InsecureSkipVerify` exists for a lab with a self-signed broker; it makes the link worth nothing
against an attacker on the path, who simply presents their own certificate.

TLS options given alongside a plain `mqtt://` URL are an error rather than being ignored, so a
misconfiguration cannot leave a peer believing a cleartext link is encrypted.

## Reconnecting

The transport owns its connection: a dropped link is redialled with exponential backoff and every
subscription is restored, with no work from the caller. `Connect` still reports an initial failure,
because a peer that cannot start is a configuration problem rather than something to retry over
silently — and a broker that refuses the client ID or the credentials is reported at once rather
than retried, since no amount of backoff changes a wrong password.

Calls in flight when a link drops fail with `TransportError` rather than being held for the
reconnection. `TransportOptions.OnConnection` reports the connection coming up and going down, and
names any subscriptions the broker refused on the way back — a peer that reconnects into a
half-working state can then say so instead of going quiet.

## Discovering Peers

Every peer publishes retained presence, so a newcomer subscribing to `<prefix>/presence/+` is handed
everyone already online. That is the whole of peer discovery on MQTT.

```go
transport := mqtt.NewTransport(mqtt.TransportOptions{
    Name:              "hmi",
    SubscribePresence: true,
    PublishPresence:   true,
    OnPresence: func(event mqtt.PresenceEvent) {
        log.Printf("%s online=%v shape=%s", event.Peer, event.Online, event.Shape)
    },
})
```

`Peers()` is the current list. `PresenceSettled(ctx)` waits until the retained burst has been read,
which is what makes "no peers found" mean something:

```go
if err := transport.PresenceSettled(ctx); err != nil {
    return err
}
for _, peer := range transport.Peers() {
    // ...
}
```

The broker delivers everything retained the moment the subscription is granted and there is no "that
was everyone" packet, so a short silence after the grant is the only end marker MQTT has. The gap is
armed when the subscribe is acknowledged and restarted by each presence message: an empty network
settles after one quiet period, a full one after its burst. It is a heuristic with an honest name — a
broker slower than the gap loses the race, which is what the caller's own context is for.

`Shape` announces a hash of this peer's description beside its retained `online`, so a cache holding
a `describe()` answer can tell a restart that changed the surface from one that did not. It rides as
an MQTT 5 user property rather than in the payload, because the payload has to stay the exact string
every already-deployed peer compares against.

A peer name arrives from the network, is interpolated into the topics this peer publishes to, and is
used as a map key, so names carrying wildcards, separators or control characters are refused and
reported through `OnRejected`. The same check applies to `mr-src` on every frame.

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

Signed TypeScript interop tests run when a built `@source-repo/rpc` dist is found next to this repo
as `rpc-ts` or `msgrpc`; set `MSGRPC_TEST_TS_DIST=/path/to/packages/rpc/dist/index.js` to use another
checkout.

`examples/calculator_server` serves `calculator.add` and emits `calculator.changed` with the sum.

## Remote Go Programs

`package node` exposes a guarded Go program runner for remote nodes. A node can serve the
`msgrpc.node` namespace with:

```go
service := node.NewService(node.ServiceOptions{
    Runner: node.RunnerOptions{Directory: "programs"},
})
defer service.Close()
if err := service.Expose(server); err != nil {
    log.Fatal(err)
}
```

The RPC methods are:

```text
describeRuntime()
listPrograms()
saveProgram(name, source)
readProgram(name)
deleteProgram(name)
buildProgram(name)
startProgram(name)
stopProgram(name)
programStatus(name)
programOutput(name)
```

Mutation methods require an authenticated peer. Add `server.UseAuthorizer(node.Authorizer(...))`
when a node should only accept program changes from named identities.

`examples/program_node` starts an MQTT node that exposes this namespace. Set
`SOURCE_RPC_PROGRAMS=/path/to/programs` to choose the storage directory and
`SOURCE_RPC_SHARED_SECRET=...` to enable HMAC signing. Set `SOURCE_RPC_SCRIPTABLE_BY=name1,name2`
to restrict authenticated peers that can change or run code.

See `docs/go-program-node.md` for the current node API and expected remote workflow.

## MQTT Signing

Configure `mqtt.TransportOptions.Signer` and `Verifier` to require signed MQTT 5 frames. The built-in
`mqtt.NewHMACSigner` and `mqtt.NewHMACVerifier` implement the same canonical v2 signing input as the
TypeScript package, including topic, response topic, content type, error code, contract version, TTL,
idempotency key, timestamp, nonce and raw payload bytes. A verifier also enables replay protection.

Verified MQTT identities are available to handlers through `rpc.CallContextFromContext(ctx)`. Mark a
method with `rpc.RequiresAuthority(true)` to require an authenticated identity, and use
`server.UseAuthorizer` for application-specific allow/deny decisions.

Use `mqtt.TransportOptions.OnRejected` to observe dropped MQTT frames, including unsigned, badly
signed, replayed, malformed and unsupported-content-type frames. Events can also be marked with
`RequiresAuthority` in their `EventDescription`; unauthorized subscription attempts are refused.
