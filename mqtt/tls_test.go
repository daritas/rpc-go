package mqtt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTLSSchemesGetTheirOwnDefaultPort(t *testing.T) {
	for _, scheme := range []string{"mqtts", "tls", "ssl"} {
		parsed, err := brokerURL(scheme + "://broker.local")
		if err != nil {
			t.Fatalf("%s:// was refused: %v", scheme, err)
		}
		if parsed.Port() != defaultTLSPort {
			t.Fatalf("%s:// port = %q, want %s", scheme, parsed.Port(), defaultTLSPort)
		}
	}
	plain, err := brokerURL("mqtt://broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if plain.Port() != defaultPlainPort {
		t.Fatalf("mqtt:// port = %q, want %s", plain.Port(), defaultPlainPort)
	}
	// WebSocket stays out of this implementation's surface.
	for _, scheme := range []string{"ws", "wss", "http"} {
		if _, err := brokerURL(scheme + "://broker.local"); err == nil {
			t.Fatalf("%s:// was accepted", scheme)
		}
	}
}

// The safe default matters more than the configurable case: an mqtts:// URL with nothing else said
// must still verify the broker, under the name in the URL.
func TestNilTLSOptionsStillVerifyTheBroker(t *testing.T) {
	config, err := (*TLSOptions)(nil).config("broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if config.InsecureSkipVerify {
		t.Fatal("verification was off by default")
	}
	if config.ServerName != "broker.local" {
		t.Fatalf("ServerName = %q, want the host from the URL", config.ServerName)
	}
	if config.RootCAs != nil {
		t.Fatal("RootCAs was set, want the host trust store")
	}
	if config.MinVersion < tls.VersionTLS12 {
		t.Fatalf("MinVersion = %#x, want TLS 1.2 or better", config.MinVersion)
	}
}

func TestTLSOptionsBuildTheConfig(t *testing.T) {
	authority := newTestAuthority(t)
	directory := t.TempDir()
	caFile := filepath.Join(directory, "ca.pem")
	if err := os.WriteFile(caFile, authority.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := (&TLSOptions{CAFile: caFile, ServerName: "elsewhere"}).config("broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if config.RootCAs == nil {
		t.Fatal("RootCAs was not built from the CA file")
	}
	if config.ServerName != "elsewhere" {
		t.Fatalf("ServerName = %q, want the override", config.ServerName)
	}

	// The same bundle in memory.
	inMemory, err := (&TLSOptions{CACertificates: authority.certPEM}).config("broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if inMemory.RootCAs == nil {
		t.Fatal("RootCAs was not built from CACertificates")
	}

	// A supplied Config wins outright.
	own := &tls.Config{ServerName: "mine"}
	got, err := (&TLSOptions{Config: own, InsecureSkipVerify: true}).config("broker.local")
	if err != nil {
		t.Fatal(err)
	}
	if got != own {
		t.Fatal("a supplied Config was not used as it stands")
	}
}

func TestTLSOptionsRefuseIncoherentInput(t *testing.T) {
	if _, err := (&TLSOptions{CertificateFile: "cert.pem"}).config("broker.local"); err == nil {
		t.Fatal("a client certificate without its key was accepted")
	}
	if _, err := (&TLSOptions{KeyFile: "key.pem"}).config("broker.local"); err == nil {
		t.Fatal("a key without its certificate was accepted")
	}
	if _, err := (&TLSOptions{CAFile: filepath.Join(t.TempDir(), "absent.pem")}).config("broker.local"); err == nil {
		t.Fatal("a missing CA file was accepted")
	}
	if _, err := (&TLSOptions{CACertificates: []byte("not a certificate")}).config("broker.local"); err == nil {
		t.Fatal("a CA bundle holding no certificate was accepted")
	}
}

// TLS options against a plain mqtt:// URL are refused rather than ignored: quietly dropping them
// would leave the caller believing the link is encrypted while the password goes over in the clear.
func TestTLSOptionsAgainstAPlainURLAreRefused(t *testing.T) {
	transport := NewTransport(TransportOptions{
		Name:     "plant",
		URL:      "mqtt://localhost:1883",
		Password: "secret",
		TLS:      &TLSOptions{InsecureSkipVerify: true},
	})
	err := transport.Connect(context.Background())
	if err == nil {
		t.Fatal("TLS options were accepted against a plain mqtt:// URL")
	}
	if !strings.Contains(err.Error(), "mqtts://") {
		t.Fatalf("err = %v, want it to point at mqtts://", err)
	}
}

// The end of the story: an mqtts:// URL reaches a listener over a real, verified TLS handshake.
// The listener speaks no MQTT, so CONNECT fails — by which point TLS has already done its job.
func TestConnectHandshakesTLSAgainstTheBroker(t *testing.T) {
	authority := newTestAuthority(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{authority.serverCertificate},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	handshakes := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			handshakes <- err
			return
		}
		defer conn.Close()
		handshakes <- conn.(*tls.Conn).HandshakeContext(context.Background())
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	transport := NewTransport(TransportOptions{
		Name:           "plant",
		URL:            "mqtts://localhost:" + port,
		ConnectTimeout: time.Second,
		// localhost resolves to 127.0.0.1, and the certificate is issued for localhost.
		TLS: &TLSOptions{CACertificates: authority.certPEM},
	})
	// Expected to fail: nothing on the far side answers a CONNECT packet.
	_ = transport.Connect(context.Background())

	select {
	case err := <-handshakes:
		if err != nil {
			t.Fatalf("the broker side of the TLS handshake failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no TLS handshake reached the broker")
	}
}

// An untrusted certificate must stop the connection rather than be shrugged off.
func TestConnectRefusesAnUntrustedBrokerCertificate(t *testing.T) {
	authority := newTestAuthority(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{authority.serverCertificate},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_ = conn.(*tls.Conn).HandshakeContext(context.Background())
			}()
		}
	}()

	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	// No CA supplied, so the host trust store is used and this self-signed certificate is unknown.
	transport := NewTransport(TransportOptions{
		Name:           "plant",
		URL:            "mqtts://localhost:" + port,
		ConnectTimeout: time.Second,
	})
	if err := transport.Connect(context.Background()); err == nil {
		t.Fatal("an untrusted broker certificate was accepted")
	}
}

type testAuthority struct {
	certPEM           []byte
	serverCertificate tls.Certificate
}

// newTestAuthority mints a self-signed certificate for localhost, standing in for a plant that runs
// its own certificate authority.
func newTestAuthority(t *testing.T) testAuthority {
	t.Helper()
	certPEM, keyPEM := selfSignedLocalhost(t)
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	// Confirm the PEM parses as a root, so a failure here is not mistaken for a handshake failure.
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		t.Fatal("the test certificate is not valid PEM")
	}
	return testAuthority{certPEM: certPEM, serverCertificate: certificate}
}

func selfSignedLocalhost(t *testing.T) (certPEM []byte, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
