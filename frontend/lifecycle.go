// Package frontend binds public Minecraft transports. It is independent of the
// private Spectrum backend Transport and performs no version conversion.
package frontend

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Limits bounds work before Minecraft authentication and pack delivery complete.
// These limits are independent of MaximumPlayers and the backend spawn timeout.
type Limits struct {
	MaxPending   int
	LoginTimeout time.Duration
	PackTimeout  time.Duration
}

func (l Limits) defaults() Limits {
	if l.MaxPending <= 0 {
		l.MaxPending = 128
	}
	if l.LoginTimeout <= 0 {
		l.LoginTimeout = 10 * time.Second
	}
	if l.PackTimeout <= 0 {
		l.PackTimeout = 2 * time.Minute
	}
	return l
}

// RakNet returns a Spectrum.ListenWith factory. MaximumMTU applies only here.
func RakNet(address string, limits Limits) func(minecraft.ListenConfig) (*minecraft.Listener, error) {
	limits = limits.defaults()
	return func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		cfg.LoginTimeout = limits.LoginTimeout
		network := (minecraft.RakNet{}).WithMaximumMTU(cfg.MaximumMTU)
		cfg.MaximumMTU = 0
		return cfg.ListenNetwork(&ownedNetwork{Network: network, limits: limits}, address)
	}
}

type bootstrapKey struct{}

// CompleteBootstrap releases the pending slot and pack deadline for an accepted
// Minecraft connection. Spectrum calls this before constructing its session.
func CompleteBootstrap(conn *minecraft.Conn) {
	if c, ok := conn.Context().Value(bootstrapKey{}).(*ownedConn); ok {
		c.complete()
	}
}

type ownedNetwork struct {
	minecraft.Network
	limits  Limits
	cleanup func() error
}

func (n *ownedNetwork) Listen(address string) (minecraft.NetworkListener, error) {
	l, err := n.Network.Listen(address)
	if err != nil {
		return nil, err
	}
	return &ownedListener{NetworkListener: l, limits: n.limits.defaults(), cleanup: n.cleanup,
		connections: make(map[*ownedConn]struct{}), done: make(chan struct{})}, nil
}

type ownedListener struct {
	minecraft.NetworkListener
	limits      Limits
	cleanup     func() error
	mu          sync.Mutex
	connections map[*ownedConn]struct{}
	pending     int
	closing     bool
	done        chan struct{}
	err         error
}

func (l *ownedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.NetworkListener.Accept()
		if err != nil {
			return nil, err
		}
		l.mu.Lock()
		if l.closing {
			l.mu.Unlock()
			_ = conn.Close()
			return nil, net.ErrClosed
		}
		if l.pending >= l.limits.MaxPending {
			l.mu.Unlock()
			_ = conn.Close()
			continue
		}
		c := &ownedConn{Conn: conn, owner: l}
		parent := context.Background()
		if contextConn, ok := conn.(interface{ Context() context.Context }); ok {
			parent = contextConn.Context()
		}
		c.ctx, c.cancel = context.WithCancel(context.WithValue(parent, bootstrapKey{}, c))
		c.timer = time.AfterFunc(l.limits.PackTimeout, func() { _ = c.Close() })
		l.connections[c] = struct{}{}
		l.pending++
		l.mu.Unlock()
		if reader, ok := conn.(packet.PacketReader); ok {
			return &ownedPacketConn{ownedConn: c, PacketReader: reader}, nil
		}
		return c, nil
	}
}

func (l *ownedListener) Close() error {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		<-l.done
		return l.err
	}
	l.closing = true
	connections := make([]*ownedConn, 0, len(l.connections))
	for c := range l.connections {
		connections = append(connections, c)
	}
	l.mu.Unlock()
	// Stop signaling before releasing peer transports and the shared ICE socket.
	var err error
	if l.cleanup != nil {
		err = l.cleanup()
	}
	err = errors.Join(err, l.NetworkListener.Close())
	for _, c := range connections {
		err = errors.Join(err, c.Close())
	}
	l.err = err
	close(l.done)
	return err
}

type ownedConn struct {
	net.Conn
	owner     *ownedListener
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *time.Timer
	completed bool // guarded by owner.mu
	once      sync.Once
	err       error
}

type ownedPacketConn struct {
	*ownedConn
	packet.PacketReader
}

func (c *ownedConn) complete() {
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	if !c.completed {
		c.completed = true
		c.owner.pending--
		c.timer.Stop()
	}
}

func (c *ownedConn) Close() error {
	c.once.Do(func() {
		c.complete()
		c.cancel()
		c.err = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.connections, c)
		c.owner.mu.Unlock()
	})
	return c.err
}

func (c *ownedConn) Context() context.Context { return c.ctx }
func (c *ownedConn) BatchHeader() []byte {
	if v, ok := c.Conn.(packet.BatchHeaderer); ok {
		return v.BatchHeader()
	}
	return []byte{0xfe}
}
func (c *ownedConn) DisableEncryption() bool {
	if v, ok := c.Conn.(packet.EncryptionDisabler); ok {
		return v.DisableEncryption()
	}
	return false
}
func (c *ownedConn) ProtocolVersion() byte {
	if v, ok := c.Conn.(interface{ ProtocolVersion() byte }); ok {
		return v.ProtocolVersion()
	}
	return 11
}
func (c *ownedConn) Latency() time.Duration {
	if v, ok := c.Conn.(interface{ Latency() time.Duration }); ok {
		return v.Latency()
	}
	return 0
}
func (c *ownedConn) PublicKey() *ecdsa.PublicKey {
	if v, ok := c.Conn.(interface{ PublicKey() *ecdsa.PublicKey }); ok {
		return v.PublicKey()
	}
	return nil
}
func (c *ownedConn) VerifyPublicKey(pub *ecdsa.PublicKey) error {
	if v, ok := c.Conn.(interface{ VerifyPublicKey(*ecdsa.PublicKey) error }); ok {
		return v.VerifyPublicKey(pub)
	}
	if key := c.PublicKey(); key != nil && !key.Equal(pub) {
		return errors.New("transport identity public key mismatch")
	}
	return nil
}
