package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
)

const defaultTLSPort = "8883"
const defaultPlainPort = "1883"

// TLSOptions configures the encrypted link to a broker reached over mqtts://.
//
// Signing and TLS answer different questions and neither replaces the other. A signature says who
// wrote a frame and that nobody altered it; it does nothing to hide it. A link crossing a trust
// boundary in cleartext gives away every payload, every method name, and the username and password
// in the CONNECT packet, to anyone on the path. Where signing is warranted, so is this.
type TLSOptions struct {
	// Config is used as it stands when set, and every other field here is ignored. For whatever the
	// convenience fields below do not reach.
	Config *tls.Config

	// CAFile names a PEM bundle of root certificates to verify the broker against. Without one the
	// host's own trust store is used, which is right for a broker holding a public certificate and
	// wrong for a plant that runs its own authority.
	CAFile string

	// CACertificates is the same bundle in memory, for a root that does not come from a file.
	CACertificates []byte

	// CertificateFile and KeyFile present a client certificate, for a broker that identifies its
	// peers by certificate rather than by username and password.
	CertificateFile string
	KeyFile         string

	// ServerName overrides the name checked against the broker's certificate. Needed when the
	// broker is reached by IP address, or through a tunnel whose host is not the broker's own name.
	// Left empty, the host from the broker URL is used.
	ServerName string

	// InsecureSkipVerify accepts whatever certificate the broker offers, without checking it.
	//
	// This makes the link worth nothing against an attacker who can sit on the path: they present
	// their own certificate, the handshake succeeds, and they read and rewrite every frame. It is
	// here for a lab with a self-signed broker, and for nowhere that matters.
	InsecureSkipVerify bool
}

// tlsSchemes are the URL schemes that mean an encrypted link, matching what the connection manager
// will dial, so a URL it would accept is not refused here first.
var tlsSchemes = map[string]bool{
	"mqtts":    true,
	"tls":      true,
	"ssl":      true,
	"mqtt+ssl": true,
	"tcps":     true,
}

var plainSchemes = map[string]bool{
	"mqtt": true,
	"tcp":  true,
}

func isTLSScheme(scheme string) bool {
	return tlsSchemes[strings.ToLower(scheme)]
}

func isPlainScheme(scheme string) bool {
	return plainSchemes[strings.ToLower(scheme)]
}

// config builds the tls.Config for a broker at serverName.
//
// A nil TLSOptions is not an error: an mqtts:// URL with nothing else said verifies the broker
// against the host's trust store under the name in the URL, which is the right default and the one
// that fails closed.
func (o *TLSOptions) config(serverName string) (*tls.Config, error) {
	if o != nil && o.Config != nil {
		return o.Config, nil
	}
	config := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if o == nil {
		return config, nil
	}
	if o.ServerName != "" {
		config.ServerName = o.ServerName
	}
	config.InsecureSkipVerify = o.InsecureSkipVerify

	roots, err := o.rootCAs()
	if err != nil {
		return nil, err
	}
	config.RootCAs = roots

	if o.CertificateFile != "" || o.KeyFile != "" {
		if o.CertificateFile == "" || o.KeyFile == "" {
			return nil, errors.New("mqtt transport: a client certificate needs both CertificateFile and KeyFile")
		}
		certificate, err := tls.LoadX509KeyPair(o.CertificateFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt transport: client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

// rootCAs assembles the certificate pool, or nil to mean the host's trust store.
func (o *TLSOptions) rootCAs() (*x509.CertPool, error) {
	if o.CAFile == "" && len(o.CACertificates) == 0 {
		return nil, nil
	}
	pool := x509.NewCertPool()
	if len(o.CACertificates) > 0 {
		if !pool.AppendCertsFromPEM(o.CACertificates) {
			return nil, errors.New("mqtt transport: CACertificates held no PEM certificate")
		}
	}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt transport: CA file: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("mqtt transport: CA file %q held no PEM certificate", o.CAFile)
		}
	}
	return pool, nil
}
