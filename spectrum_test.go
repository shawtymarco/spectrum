package spectrum

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cooldogedev/spectrum/frontend"
	sp "github.com/cooldogedev/spectrum/protocol"
	"github.com/cooldogedev/spectrum/server"
	spk "github.com/cooldogedev/spectrum/server/packet"
	"github.com/cooldogedev/spectrum/session"
	"github.com/cooldogedev/spectrum/session/animation"
	"github.com/cooldogedev/spectrum/util"
	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/golang/snappy"
	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type backendStub struct {
	wg      sync.WaitGroup
	spawned chan string
	calls   atomic.Int32
}

func (b *backendStub) Dial(ctx context.Context, addr string) (io.ReadWriteCloser, error) {
	b.calls.Add(1)
	if addr == "blocked" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client, backend := net.Pipe()
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer backend.Close()
		reader, writer := sp.NewReader(backend), sp.NewWriter(backend)
		readID := func() (uint32, error) {
			data, err := reader.ReadPacket()
			if err != nil {
				return 0, err
			}
			data, err = snappy.Decode(nil, data)
			if err != nil {
				return 0, err
			}
			h := new(packet.Header)
			err = h.Read(bytes.NewBuffer(data))
			return h.PacketID, err
		}
		write := func(pk packet.Packet) error {
			buf := new(bytes.Buffer)
			_ = (&packet.Header{PacketID: pk.ID()}).Write(buf)
			pk.Marshal(protocol.NewWriter(buf, 0))
			return writer.Write(append([]byte{0}, snappy.Encode(nil, buf.Bytes())...))
		}
		if id, err := readID(); err != nil || id != spk.IDConnectionRequest {
			return
		}
		if addr == "reject" {
			return
		}
		for _, pk := range []packet.Packet{&spk.ConnectionResponse{RuntimeID: 1, UniqueID: 1}, &packet.StartGame{WorldName: addr, EntityRuntimeID: 1, EntityUniqueID: 1}, &packet.ItemRegistry{}} {
			if write(pk) != nil {
				return
			}
		}
		if id, err := readID(); err != nil || id != packet.IDRequestChunkRadius {
			return
		}
		if write(&packet.ChunkRadiusUpdated{ChunkRadius: 8}) != nil {
			return
		}
		if write(&packet.PlayStatus{Status: packet.PlayStatusPlayerSpawn}) != nil {
			return
		}
		if id, err := readID(); err != nil || id != packet.IDSetLocalPlayerAsInitialised {
			return
		}
		b.spawned <- addr
		for {
			if _, err := readID(); err != nil {
				return
			}
		}
	}()
	return client, nil
}

func dualProxy(t *testing.T, maxPlayers int, auto bool, backendAddress string) (*Spectrum, *backendStub) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := util.DefaultOpts()
	opts.AutoLogin = auto
	b := &backendStub{spawned: make(chan string, 16)}
	s := NewSpectrum(server.NewStaticDiscovery(backendAddress, backendAddress), logger, opts, b)
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	err = s.ListenWith(minecraft.ListenConfig{AuthenticationDisabled: true, MaximumPlayers: maxPlayers, MaximumMTU: 1200, ErrorLog: logger},
		frontend.RakNet("127.0.0.1:0", frontend.Limits{}),
		frontend.NetherNet(frontend.NetherNetConfig{SignalingAddress: "127.0.0.1:0", ICEAddress: "127.0.0.1:0", Identity: key, AllowAnonymous: true}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); b.wg.Wait() })
	return s, b
}

func dialPublic(t *testing.T, s *Spectrum, transport int, identity string) (*minecraft.Conn, *session.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	accepted := make(chan *session.Session, 1)
	go func() { ss, _ := s.Accept(); accepted <- ss }()
	var network minecraft.Network = minecraft.RakNet{}
	address := s.Listeners()[transport].Addr().String()
	if transport == 1 {
		var settings webrtc.SettingEngine
		settings.SetIncludeLoopbackCandidate(true)
		network = minecraft.NetherNet{Dialer: nethernet.Dialer{API: webrtc.NewAPI(webrtc.WithSettingEngine(settings))}}
		address = "http://" + address
	}
	d := minecraft.Dialer{IdentityData: login.IdentityData{Identity: identity, DisplayName: "TransportTest"}}
	conn, err := d.DialContextNetwork(ctx, network, address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	select {
	case ss := <-accepted:
		if ss == nil {
			t.Fatal("accept failed")
		}
		return conn, ss
	case <-ctx.Done():
		t.Fatal("accept timed out")
	}
	return nil, nil
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDualTransportSessionsAndBackendTransfer(t *testing.T) {
	s, b := dualProxy(t, 4, true, "lobby")
	clients := make([]*minecraft.Conn, 2)
	sessions := make([]*session.Session, 2)
	for transport := range 2 {
		clients[transport], sessions[transport] = dialPublic(t, s, transport, uuid.NewString())
		if err := clients[transport].DoSpawnTimeout(5 * time.Second); err != nil {
			t.Fatal(err)
		}
		sessions[transport].SetAnimation(animation.NopAnimation{})
		go func(c *minecraft.Conn) {
			for {
				if _, err := c.ReadPacket(); err != nil {
					return
				}
			}
		}(clients[transport])
	}
	eventually(t, func() bool { return len(s.Registry().GetSessions()) == 2 })
	for _, l := range s.Listeners() {
		if l.PlayerCount() != 2 {
			t.Fatalf("shared player count = %d", l.PlayerCount())
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pong, err := (minecraft.NetherNet{}).PingContext(ctx, "http://"+s.Listeners()[1].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	status, err := endpoint.RakNetPongData(pong)
	if err != nil || status.PlayerCount != 2 || status.MaxPlayerCount != 4 {
		t.Fatalf("status %+v: %v", status, err)
	}
	for i, ss := range sessions {
		public := ss.Client()
		if err := ss.Transfer("game"); err != nil {
			t.Fatal(err)
		}
		eventually(t, func() bool { return ss.Server().Spawned() })
		if ss.Client() != public || ss.Client().Context().Err() != nil || ss.Client().Proto().ID() != minecraft.DefaultProtocol.ID() {
			t.Fatalf("transport %d lost frontend or protocol", i)
		}
	}
	for range 4 {
		select {
		case <-b.spawned:
		case <-ctx.Done():
			t.Fatal("backend spawn missing")
		}
	}
}

func TestDualTransportDuplicateAccount(t *testing.T) {
	s, _ := dualProxy(t, 3, true, "lobby")
	identity := uuid.NewString()
	first, old := dialPublic(t, s, 0, identity)
	if err := first.DoSpawnTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return s.Registry().GetSession(identity) == old })
	second, replacement := dialPublic(t, s, 1, identity)
	if err := second.DoSpawnTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return old.Context().Err() != nil && s.Registry().GetSession(identity) == replacement })
	_ = old.Close()
	if s.Registry().GetSession(identity) != replacement {
		t.Fatal("retired connection erased replacement")
	}
}

func TestDualTransportConcurrentAdmission(t *testing.T) {
	s, _ := dualProxy(t, 2, true, "lobby")
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	accepted := make(chan error, 2)
	go func() {
		for range 2 {
			_, err := s.Accept()
			accepted <- err
		}
	}()
	type result struct {
		conn *minecraft.Conn
		err  error
	}
	results := make(chan result, 2)
	for transport := range 2 {
		go func() {
			var network minecraft.Network = minecraft.RakNet{}
			address := s.Listeners()[transport].Addr().String()
			if transport == 1 {
				var settings webrtc.SettingEngine
				settings.SetIncludeLoopbackCandidate(true)
				network = minecraft.NetherNet{Dialer: nethernet.Dialer{API: webrtc.NewAPI(webrtc.WithSettingEngine(settings))}}
				address = "http://" + address
			}
			c, err := (minecraft.Dialer{IdentityData: login.IdentityData{Identity: uuid.NewString(), DisplayName: "Concurrent"}}).DialContextNetwork(ctx, network, address)
			if err == nil {
				err = c.DoSpawnContext(ctx)
			}
			results <- result{c, err}
		}()
	}
	for range 2 {
		select {
		case r := <-results:
			if r.conn != nil {
				defer r.conn.Close()
			}
			if r.err != nil {
				t.Fatal(r.err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent dials stalled")
		}
	}
	for range 2 {
		if err := <-accepted; err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool { return len(s.Registry().GetSessions()) == 2 })
	if s.Listener().PlayerCount() != 2 {
		t.Fatal("shared admission lost a connection")
	}
}

func TestInitialBackendRejectionDoesNotLoopFallback(t *testing.T) {
	s, b := dualProxy(t, 2, true, "reject")
	accepted := make(chan error, 1)
	go func() { _, err := s.Accept(); accepted <- err }()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	client, err := minecraft.DialContext(ctx, "raknet", s.Listener().Addr().String())
	if client != nil {
		_ = client.Close()
	}
	if err == nil {
		t.Fatal("rejected backend completed login")
	}
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("public accept did not complete")
	}
	if b.calls.Load() != 1 {
		t.Fatalf("initial handshake retried %d times", b.calls.Load())
	}
	if len(s.Registry().GetSessions()) != 0 {
		t.Fatal("failed login published a session")
	}
}

func TestDualTransportSharedLimit(t *testing.T) {
	s, _ := dualProxy(t, 1, true, "lobby")
	_, _ = dialPublic(t, s, 0, uuid.NewString())
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var settings webrtc.SettingEngine
	settings.SetIncludeLoopbackCandidate(true)
	network := minecraft.NetherNet{Dialer: nethernet.Dialer{API: webrtc.NewAPI(webrtc.WithSettingEngine(settings))}}
	conn, err := (minecraft.Dialer{}).DialContextNetwork(ctx, network, "http://"+s.Listeners()[1].Addr().String())
	if err == nil {
		_ = conn.Close()
		t.Fatal("second transport bypassed shared limit")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("capacity test never reached rejection: %v", err)
	}
	if s.Listener().PlayerCount() != 1 {
		t.Fatal("rejected connection changed shared count")
	}
}

func TestDualTransportShutdownCancelsInitialization(t *testing.T) {
	for _, auto := range []bool{false, true} {
		t.Run(map[bool]string{false: "manual", true: "auto"}[auto], func(t *testing.T) {
			s, _ := dualProxy(t, 0, auto, "blocked")
			address := s.Listeners()[1].Addr().String()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			dialDone := make(chan error, 1)
			go func() {
				var settings webrtc.SettingEngine
				settings.SetIncludeLoopbackCandidate(true)
				network := minecraft.NetherNet{Dialer: nethernet.Dialer{API: webrtc.NewAPI(webrtc.WithSettingEngine(settings))}}
				c, err := (minecraft.Dialer{}).DialContextNetwork(ctx, network, "http://"+address)
				if c != nil {
					_ = c.Close()
				}
				dialDone <- err
			}()
			pending := make(chan *session.Session, 1)
			go func() { ss, _ := s.Accept(); pending <- ss }()
			var ss *session.Session
			select {
			case ss = <-pending:
				if ss == nil {
					t.Fatal("accept failed")
				}
			case err := <-dialDone:
				t.Fatalf("dial before accept: %v", err)
			case <-ctx.Done():
				t.Fatal("accept timed out")
			}
			accepted := make(chan error, 1)
			go func() { _, err := s.Accept(); accepted <- err }()
			closed := make(chan error, 1)
			go func() { closed <- s.Close() }()
			select {
			case <-closed:
			case <-time.After(3 * time.Second):
				t.Fatal("close blocked on initialization")
			}
			if ss.Context().Err() == nil {
				t.Fatal("pre-registry session survived shutdown")
			}
			if err := <-accepted; !errors.Is(err, net.ErrClosed) {
				t.Fatalf("accept after shutdown: %v", err)
			}
			tcp, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatalf("HTTP socket leaked: %v", err)
			}
			_ = tcp.Close()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			cancel()
			select {
			case <-dialDone:
			case <-time.After(3 * time.Second):
				t.Fatal("dial did not terminate")
			}
		})
	}
}

func TestListenWithRollbackAndRetry(t *testing.T) {
	s := NewSpectrum(nil, nil, nil, nil)
	defer s.Close()
	var address string
	first := func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		l, err := cfg.Listen("raknet", "127.0.0.1:0")
		if l != nil {
			address = l.Addr().String()
		}
		return l, err
	}
	failure := func(minecraft.ListenConfig) (*minecraft.Listener, error) { return nil, errors.New("bind failed") }
	if err := s.ListenWith(minecraft.ListenConfig{AuthenticationDisabled: true}, first, failure); err == nil {
		t.Fatal("expected bind failure")
	}
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("rollback leaked UDP socket: %v", err)
	}
	_ = l.Close()
	if len(s.Listeners()) != 0 {
		t.Fatal("partial listeners published")
	}
	if err := s.ListenWith(minecraft.ListenConfig{AuthenticationDisabled: true}, first); err != nil {
		t.Fatal(err)
	}
}

func TestDualTransportRealBindRollback(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	s := NewSpectrum(nil, nil, nil, nil)
	defer s.Close()
	var raknetAddress string
	rak := frontend.RakNet("127.0.0.1:0", frontend.Limits{})
	err = s.ListenWith(minecraft.ListenConfig{AuthenticationDisabled: true}, func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		l, err := rak(cfg)
		if l != nil {
			raknetAddress = l.Addr().String()
		}
		return l, err
	}, frontend.NetherNet(frontend.NetherNetConfig{SignalingAddress: occupied.Addr().String(), ICEAddress: "127.0.0.1:0", Identity: key, AllowAnonymous: true}))
	if err == nil {
		t.Fatal("occupied signaling port was accepted")
	}
	addr, err := net.ResolveUDPAddr("udp", raknetAddress)
	if err != nil {
		t.Fatal(err)
	}
	udp, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("RakNet socket survived failed NetherNet bind: %v", err)
	}
	_ = udp.Close()

	// A later failure must also unwind already bound HTTP and ICE resources.
	ice, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	iceAddress := ice.LocalAddr().(*net.UDPAddr)
	_ = ice.Close()
	var httpAddress string
	nether := frontend.NetherNet(frontend.NetherNetConfig{SignalingAddress: "127.0.0.1:0", ICEAddress: iceAddress.String(), Identity: key, AllowAnonymous: true})
	err = s.ListenWith(minecraft.ListenConfig{AuthenticationDisabled: true}, func(cfg minecraft.ListenConfig) (*minecraft.Listener, error) {
		l, err := nether(cfg)
		if l != nil {
			httpAddress = l.Addr().String()
		}
		return l, err
	}, func(minecraft.ListenConfig) (*minecraft.Listener, error) {
		return nil, errors.New("later bind failure")
	})
	if err == nil {
		t.Fatal("expected later bind failure")
	}
	tcp, err := net.Listen("tcp", httpAddress)
	if err != nil {
		t.Fatalf("HTTP socket leaked: %v", err)
	}
	_ = tcp.Close()
	udp, err = net.ListenUDP("udp", iceAddress)
	if err != nil {
		t.Fatalf("ICE socket leaked: %v", err)
	}
	_ = udp.Close()
}
