package frontend

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type capabilityConn struct {
	net.Conn
	key *ecdsa.PublicKey
}

func (c capabilityConn) BatchHeader() []byte         { return nil }
func (c capabilityConn) DisableEncryption() bool     { return true }
func (c capabilityConn) ReadPacket() ([]byte, error) { return []byte("complete frame"), nil }
func (c capabilityConn) ProtocolVersion() byte       { return 10 }
func (c capabilityConn) Latency() time.Duration      { return 12 * time.Millisecond }
func (c capabilityConn) PublicKey() *ecdsa.PublicKey { return c.key }
func (c capabilityConn) VerifyPublicKey(key *ecdsa.PublicKey) error {
	if c.key.Equal(key) {
		return nil
	}
	return errors.New("identity mismatch")
}

type rawListener struct {
	incoming chan net.Conn
	done     chan struct{}
}

func (l *rawListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.incoming:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *rawListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}
func (l *rawListener) Addr() net.Addr { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)} }
func (*rawListener) ID() int64        { return 1 }
func (*rawListener) PongData([]byte)  {}

func TestOwnedConnectionPreservesCapabilities(t *testing.T) {
	key, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	other, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	a, b := net.Pipe()
	defer b.Close()
	raw := &rawListener{incoming: make(chan net.Conn, 1), done: make(chan struct{})}
	raw.incoming <- capabilityConn{Conn: a, key: &key.PublicKey}
	l := &ownedListener{NetworkListener: raw, limits: Limits{MaxPending: 1, PackTimeout: time.Minute}, connections: make(map[*ownedConn]struct{}), done: make(chan struct{})}
	defer l.Close()
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	capabilities, ok := conn.(packet.TransportCapabilities)
	if !ok {
		t.Fatal("packet capabilities hidden")
	}
	frame, err := capabilities.ReadPacket()
	if err != nil || string(frame) != "complete frame" || len(capabilities.BatchHeader()) != 0 || !capabilities.DisableEncryption() {
		t.Fatal("framing/encryption changed")
	}
	if conn.(interface{ ProtocolVersion() byte }).ProtocolVersion() != 10 {
		t.Fatal("legacy RakNet negotiation hidden")
	}
	if conn.(interface{ Latency() time.Duration }).Latency() != 12*time.Millisecond {
		t.Fatal("latency hidden")
	}
	verify := conn.(interface{ VerifyPublicKey(*ecdsa.PublicKey) error })
	if verify.VerifyPublicKey(&key.PublicKey) != nil || verify.VerifyPublicKey(&other.PublicKey) == nil {
		t.Fatal("public-key binding lost")
	}
	conn.(interface{ Context() context.Context }).Context().Value(bootstrapKey{}).(*ownedConn).complete()
	if l.pending != 0 {
		t.Fatal("bootstrap slot leaked")
	}
}

func TestPendingPackDeadlineAndClose(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	raw := &rawListener{incoming: make(chan net.Conn, 1), done: make(chan struct{})}
	raw.incoming <- a
	l := &ownedListener{NetworkListener: raw, limits: Limits{MaxPending: 1, PackTimeout: 20 * time.Millisecond}, connections: make(map[*ownedConn]struct{}), done: make(chan struct{})}
	defer l.Close()
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	ctx := conn.(interface{ Context() context.Context }).Context()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("pack bootstrap never expired")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}
