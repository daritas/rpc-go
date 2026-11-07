package mqtt

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"
	"time"

	rpc "github.com/daritas/rpc-go"
)

const (
	defaultMaxClockSkew     = time.Minute
	defaultMaxTrackedNonces = 5000
)

type MessageSigner interface {
	Sign(context.Context, []byte, SigningContext) (string, error)
}

type MessageVerifier interface {
	Verify(context.Context, []byte, string, SigningContext) (*rpc.Identity, error)
}

type SigningContext struct {
	Source string
}

type SignedFrameV5 struct {
	Version         string
	Topic           string
	ResponseTopic   string
	Source          string
	Kind            string
	Path            string
	MethodOrEvent   string
	Correlation     string
	ContentType     string
	Code            string
	ContractVersion string
	TTL             string
	IdempotencyKey  string
	Timestamp       int64
	Nonce           string
	Payload         []byte
}

func CanonicalSignedBytesV5(frame SignedFrameV5) ([]byte, error) {
	var preamble bytes.Buffer
	encoder := json.NewEncoder(&preamble)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode([]any{
		frame.Version,
		frame.Topic,
		frame.ResponseTopic,
		frame.Source,
		frame.Kind,
		frame.Path,
		frame.MethodOrEvent,
		frame.Correlation,
		frame.ContentType,
		frame.Code,
		frame.ContractVersion,
		frame.TTL,
		frame.IdempotencyKey,
		frame.Timestamp,
		frame.Nonce,
	})
	if err != nil {
		return nil, err
	}
	preambleBytes := bytes.TrimSuffix(preamble.Bytes(), []byte{'\n'})
	return append(preambleBytes, frame.Payload...), nil
}

type HMACSigner struct {
	Secret []byte
}

func NewHMACSigner(secret string) HMACSigner {
	return HMACSigner{Secret: []byte(secret)}
}

func (s HMACSigner) Sign(_ context.Context, canonical []byte, _ SigningContext) (string, error) {
	if len(s.Secret) == 0 {
		return "", errors.New("empty HMAC secret")
	}
	mac := hmac.New(sha256.New, s.Secret)
	_, _ = mac.Write(canonical)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

type HMACVerifier struct {
	Secrets     map[string][]byte
	Resolve     func(string) ([]byte, bool)
	IdentityFor func(string) *rpc.Identity
}

func NewHMACVerifier(secrets map[string]string) HMACVerifier {
	converted := make(map[string][]byte, len(secrets))
	for source, secret := range secrets {
		converted[source] = []byte(secret)
	}
	return HMACVerifier{Secrets: converted}
}

func (v HMACVerifier) Verify(_ context.Context, canonical []byte, signature string, context SigningContext) (*rpc.Identity, error) {
	secret, ok := v.secretFor(context.Source)
	if !ok || len(secret) == 0 {
		return nil, nil
	}
	actual, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return nil, nil
	}
	mac := hmac.New(sha256.New, secret)
	_, _ = mac.Write(canonical)
	if !hmac.Equal(actual, mac.Sum(nil)) {
		return nil, nil
	}
	if v.IdentityFor != nil {
		if identity := v.IdentityFor(context.Source); identity != nil {
			return identity, nil
		}
	}
	return &rpc.Identity{Name: context.Source}, nil
}

func (v HMACVerifier) secretFor(source string) ([]byte, bool) {
	if v.Resolve != nil {
		return v.Resolve(source)
	}
	secret, ok := v.Secrets[source]
	return secret, ok
}

type ReplayGuard struct {
	mu               sync.Mutex
	maxClockSkew     time.Duration
	maxTrackedNonces int
	seen             map[string]time.Time
	order            []string
	now              func() time.Time
}

func NewReplayGuard(maxClockSkew time.Duration, maxTrackedNonces int) *ReplayGuard {
	if maxClockSkew == 0 {
		maxClockSkew = defaultMaxClockSkew
	}
	if maxTrackedNonces <= 0 {
		maxTrackedNonces = defaultMaxTrackedNonces
	}
	return &ReplayGuard{
		maxClockSkew:     maxClockSkew,
		maxTrackedNonces: maxTrackedNonces,
		seen:             map[string]time.Time{},
		now:              time.Now,
	}
}

func (g *ReplayGuard) Accept(nonce string, timestampMillis int64) bool {
	if nonce == "" || timestampMillis <= 0 {
		return false
	}
	now := g.now()
	timestamp := time.UnixMilli(timestampMillis)
	if absDuration(now.Sub(timestamp)) > g.maxClockSkew {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.seen[nonce]; ok {
		return false
	}
	g.seen[nonce] = now
	g.order = append(g.order, nonce)
	g.prune(now)
	return true
}

func (g *ReplayGuard) Size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.seen)
}

func (g *ReplayGuard) prune(now time.Time) {
	for len(g.order) > 0 {
		nonce := g.order[0]
		seenAt, ok := g.seen[nonce]
		if !ok {
			g.order = g.order[1:]
			continue
		}
		if now.Sub(seenAt) <= g.maxClockSkew && len(g.seen) <= g.maxTrackedNonces {
			return
		}
		delete(g.seen, nonce)
		g.order = g.order[1:]
	}
}

func createNonce() (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(nonce[:]), nil
}

func absDuration(value time.Duration) time.Duration {
	if value < 0 {
		return -value
	}
	return value
}
