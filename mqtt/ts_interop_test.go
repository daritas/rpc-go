package mqtt

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/source-repo/rpc-go"
)

func TestSignedGoClientCallsTypeScriptServer(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))
	tsDist := requireTypeScriptRPCDist(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prefix := "msgrpc/go-ts-sign/" + rpc.NewID()
	secrets := map[string]string{
		"goSignedClient": "go-client-secret",
		"tsSignedServer": "ts-server-secret",
	}
	server := startTypeScriptPeer(t, ctx, tsDist, tsSignedServerScript(), map[string]string{
		"BROKER_URL":       brokerURL,
		"BROKER_USERNAME":  os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		"BROKER_PASSWORD":  os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		"PREFIX":           prefix,
		"GO_CLIENT_SECRET": secrets["goSignedClient"],
		"TS_SERVER_SECRET": secrets["tsSignedServer"],
		"GO_CLIENT_NAME":   "goSignedClient",
		"TS_SERVER_NAME":   "tsSignedServer",
	})
	defer server.stop()

	transport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goSignedClient",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:   NewHMACSigner(secrets["goSignedClient"]),
		Verifier: NewHMACVerifier(secrets),
	})
	if err := transport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer transport.Close(context.Background())

	client := rpc.NewClient("goSignedClient")
	client.Use(transport)

	var result string
	if err := client.Call(ctx, "tsSignedServer", "auth", "who", []any{}, &result); err != nil {
		t.Fatal(err)
	}
	if result != "goSignedClient" {
		t.Fatalf("result = %s, want goSignedClient", result)
	}
}

func TestSignedTypeScriptClientCallsGoServer(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))
	tsDist := requireTypeScriptRPCDist(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prefix := "msgrpc/ts-go-sign/" + rpc.NewID()
	secrets := map[string]string{
		"goSignedServer": "go-server-secret",
		"tsSignedClient": "ts-client-secret",
	}
	server := rpc.NewServer("goSignedServer")
	server.ExposeFunction("auth", "who", func(ctx context.Context) (string, error) {
		callContext, ok := rpc.CallContextFromContext(ctx)
		if !ok || callContext.Identity == nil {
			return "", &rpc.RPCError{Code: rpc.ErrorUnauthorized, Err: &rpc.RemoteError{Name: "RpcError", Message: "missing identity"}}
		}
		return callContext.Identity.Name, nil
	}, rpc.RequiresAuthority(true))
	serverTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goSignedServer",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:   NewHMACSigner(secrets["goSignedServer"]),
		Verifier: NewHMACVerifier(secrets),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	output := runTypeScriptPeer(t, ctx, tsDist, tsSignedClientScript(), map[string]string{
		"BROKER_URL":       brokerURL,
		"BROKER_USERNAME":  os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		"BROKER_PASSWORD":  os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		"PREFIX":           prefix,
		"GO_SERVER_SECRET": secrets["goSignedServer"],
		"TS_CLIENT_SECRET": secrets["tsSignedClient"],
		"GO_SERVER_NAME":   "goSignedServer",
		"TS_CLIENT_NAME":   "tsSignedClient",
	})
	if !strings.Contains(output, "RESULT tsSignedClient") {
		t.Fatalf("typescript client output = %q, want result", output)
	}
}

func TestTypeScriptSignedForgedAndReplayedFramesAgainstGoServer(t *testing.T) {
	brokerURL := os.Getenv("MSGRPC_TEST_BROKER")
	if brokerURL == "" {
		brokerURL = "mqtt://localhost:1883"
	}
	requireBroker(t, tcpAddress(t, brokerURL))
	tsDist := requireTypeScriptRPCDist(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	prefix := "msgrpc/ts-go-sign-negative/" + rpc.NewID()
	secrets := map[string]string{
		"goSignedServer": "go-server-secret",
		"tsSignedClient": "ts-client-secret",
	}
	var calls atomic.Int32
	server := rpc.NewServer("goSignedServer")
	server.ExposeFunction("counter", "bump", func(context.Context) (string, error) {
		return fmt.Sprintf("bump %d", calls.Add(1)), nil
	}, rpc.RequiresAuthority(true))
	serverTransport := NewTransport(TransportOptions{
		URL:      brokerURL,
		Name:     "goSignedServer",
		Prefix:   prefix,
		Username: os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		Password: os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		Signer:   NewHMACSigner(secrets["goSignedServer"]),
		Verifier: NewHMACVerifier(secrets),
	})
	serverTransport.AttachServer(server)
	if err := serverTransport.Connect(ctx); err != nil {
		skipBrokerConnect(t, err)
	}
	defer serverTransport.Close(context.Background())

	common := map[string]string{
		"BROKER_URL":       brokerURL,
		"BROKER_USERNAME":  os.Getenv("MSGRPC_TEST_BROKER_USERNAME"),
		"BROKER_PASSWORD":  os.Getenv("MSGRPC_TEST_BROKER_PASSWORD"),
		"PREFIX":           prefix,
		"GO_SERVER_NAME":   "goSignedServer",
		"TS_CLIENT_NAME":   "tsSignedClient",
		"TS_CLIENT_SECRET": secrets["tsSignedClient"],
	}
	for _, mode := range []string{"wrong-key", "tamper-content-type"} {
		env := cloneEnv(common)
		env["MODE"] = mode
		runTypeScriptPeer(t, ctx, tsDist, tsRawSignedPublishScript(), env)
		time.Sleep(300 * time.Millisecond)
		if calls.Load() != 0 {
			t.Fatalf("%s frame ran %d call(s), want 0", mode, calls.Load())
		}
	}

	env := cloneEnv(common)
	env["MODE"] = "replay"
	runTypeScriptPeer(t, ctx, tsDist, tsRawSignedPublishScript(), env)
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want one accepted frame and one replay rejection", calls.Load())
	}
}

type nodePeer struct {
	cmd    *exec.Cmd
	cancel context.CancelFunc
	stderr *bytes.Buffer
}

func (p *nodePeer) stop() {
	p.cancel()
	_ = p.cmd.Wait()
}

func startTypeScriptPeer(t *testing.T, ctx context.Context, dist string, script string, env map[string]string) *nodePeer {
	t.Helper()
	peerCtx, cancel := context.WithCancel(ctx)
	cmd := nodeCommand(t, peerCtx, dist, script, env)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if scanner.Text() == "READY" {
				ready <- nil
				return
			}
		}
		if err := scanner.Err(); err != nil {
			ready <- err
			return
		}
		ready <- fmt.Errorf("node peer exited before READY: %s", stderr.String())
	}()
	select {
	case err := <-ready:
		if err != nil {
			cancel()
			_ = cmd.Wait()
			t.Fatal(err)
		}
	case <-ctx.Done():
		cancel()
		_ = cmd.Wait()
		t.Fatalf("typescript peer did not become ready: %v\n%s", ctx.Err(), stderr.String())
	}
	return &nodePeer{cmd: cmd, cancel: cancel, stderr: &stderr}
}

func runTypeScriptPeer(t *testing.T, ctx context.Context, dist string, script string, env map[string]string) string {
	t.Helper()
	cmd := nodeCommand(t, ctx, dist, script, env)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("typescript peer failed: %v\n%s", err, output)
	}
	return string(output)
}

func nodeCommand(t *testing.T, ctx context.Context, dist string, script string, env map[string]string) *exec.Cmd {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is required for TypeScript interop tests")
	}
	path := filepath.Join(t.TempDir(), "peer.mjs")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, node, path)
	cmd.Env = append(os.Environ(), "RPC_DIST_URL="+fileURL(dist))
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	return cmd
}

func requireTypeScriptRPCDist(t *testing.T) string {
	t.Helper()
	if dist := os.Getenv("MSGRPC_TEST_TS_DIST"); dist != "" {
		if fileExists(dist) {
			return dist
		}
		t.Skipf("MSGRPC_TEST_TS_DIST does not exist: %s", dist)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Skip("cannot locate test file")
	}
	repoRoot := filepath.Dir(filepath.Dir(file))
	workspace := filepath.Dir(repoRoot)
	candidates := []string{
		filepath.Join(workspace, "rpc-ts", "packages", "rpc", "dist", "index.js"),
		filepath.Join(workspace, "msgrpc", "packages", "rpc", "dist", "index.js"),
	}
	for _, candidate := range candidates {
		if fileExists(candidate) {
			return candidate
		}
	}
	t.Skip("TypeScript @source-repo/rpc dist not found; set MSGRPC_TEST_TS_DIST")
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func fileURL(path string) string {
	absolute, err := filepath.Abs(path)
	if err != nil {
		absolute = path
	}
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(absolute)}).String()
}

func cloneEnv(values map[string]string) map[string]string {
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func tsSignedServerScript() string {
	return `
const {
  RpcServer,
  createHmacSigner,
  createHmacVerifier
} = await import(process.env.RPC_DIST_URL)

const secrets = {
  [process.env.GO_CLIENT_NAME]: process.env.GO_CLIENT_SECRET,
  [process.env.TS_SERVER_NAME]: process.env.TS_SERVER_SECRET
}
const verify = createHmacVerifier(
  (source) => secrets[source],
  (source) => ({ name: source, roles: ['operator'] })
)
class Auth {
  async who() {
    return process.env.GO_CLIENT_NAME
  }
}
const server = new RpcServer({
  name: process.env.TS_SERVER_NAME,
  transports: [{
    brokerurl: process.env.BROKER_URL,
    prefix: process.env.PREFIX,
    sessionExpirySeconds: 60,
    sign: createHmacSigner(secrets[process.env.TS_SERVER_NAME]),
    verify,
    mqtt: {
      username: process.env.BROKER_USERNAME || undefined,
      password: process.env.BROKER_PASSWORD || undefined
    }
  }],
  requireAuthenticatedPeers: true,
  authorize: ({ identity }) => identity?.name === process.env.GO_CLIENT_NAME
})
await server.ready()
server.exposeClassInstance(new Auth(), 'auth')
console.log('READY')
process.on('SIGTERM', async () => {
  await server.close()
  process.exit(0)
})
process.on('SIGINT', async () => {
  await server.close()
  process.exit(0)
})
await new Promise(() => {})
`
}

func tsSignedClientScript() string {
	return `
const {
  RpcClient,
  MqttTransport,
  createHmacSigner,
  createHmacVerifier
} = await import(process.env.RPC_DIST_URL)

const secrets = {
  [process.env.GO_SERVER_NAME]: process.env.GO_SERVER_SECRET,
  [process.env.TS_CLIENT_NAME]: process.env.TS_CLIENT_SECRET
}
const verify = createHmacVerifier(
  (source) => secrets[source],
  (source) => ({ name: source, roles: ['operator'] })
)
const client = new RpcClient(undefined, {
  name: process.env.TS_CLIENT_NAME,
  defaultTarget: process.env.GO_SERVER_NAME,
  transport: new MqttTransport(process.env.TS_CLIENT_NAME, process.env.BROKER_URL, {
    prefix: process.env.PREFIX,
    sessionExpirySeconds: 60,
    sign: createHmacSigner(secrets[process.env.TS_CLIENT_NAME]),
    verify,
    mqtt: {
      username: process.env.BROKER_USERNAME || undefined,
      password: process.env.BROKER_PASSWORD || undefined
    }
  })
})
await client.ready()
try {
  const remote = await client.proxy('auth')
  const result = await remote.who()
  console.log('RESULT ' + result)
} finally {
  await client.close()
}
`
}

func tsRawSignedPublishScript() string {
	return `
import { createRequire } from 'node:module'
const {
  canonicalSignedBytesV5,
  createHmacSigner,
  createNonce,
  msgPackCodec
} = await import(process.env.RPC_DIST_URL)
const require = createRequire(process.env.RPC_DIST_URL)
const { connectAsync } = require('mqtt')

const topic = process.env.PREFIX + '/req/' + process.env.GO_SERVER_NAME
const responseTopic = process.env.PREFIX + '/rsp/' + process.env.TS_CLIENT_NAME
const body = msgPackCodec.encode([])
const nonce = createNonce()
const timestamp = Date.now()
const mode = process.env.MODE
const correlation = 'ts-raw-' + mode
const signedContentType = mode === 'tamper-content-type' ? 'application/json' : 'application/msgpack'
const sentContentType = 'application/msgpack'
const signed = canonicalSignedBytesV5({
  version: '2',
  topic,
  responseTopic,
  source: process.env.TS_CLIENT_NAME,
  kind: 'call',
  path: 'counter',
  methodOrEvent: 'bump',
  correlation,
  contentType: signedContentType,
  code: '',
  contractVersion: '',
  ttl: '4000',
  idempotencyKey: '',
  timestamp,
  nonce,
  payload: body
})
const secret = mode === 'wrong-key' ? 'not-the-client-secret' : process.env.TS_CLIENT_SECRET
const signature = await createHmacSigner(secret)(signed, { source: process.env.TS_CLIENT_NAME })
const client = await connectAsync(process.env.BROKER_URL, {
  protocolVersion: 5,
  username: process.env.BROKER_USERNAME || undefined,
  password: process.env.BROKER_PASSWORD || undefined
})
const publish = () => client.publishAsync(topic, Buffer.from(body), {
  qos: 1,
  properties: {
    responseTopic,
    correlationData: Buffer.from(correlation),
    contentType: sentContentType,
    messageExpiryInterval: 4,
    userProperties: {
      'mr-v': '2',
      'mr-src': process.env.TS_CLIENT_NAME,
      'mr-kind': 'call',
      'mr-path': 'counter',
      'mr-method': 'bump',
      'mr-ttl': '4000',
      'mr-nonce': nonce,
      'mr-ts': String(timestamp),
      'mr-sig': signature
    }
  }
})
await publish()
if (mode === 'replay') await publish()
await client.endAsync()
console.log('DONE ' + mode)
`
}
